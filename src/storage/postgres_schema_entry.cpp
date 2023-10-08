#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_table_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

PostgresSchemaEntry::PostgresSchemaEntry(Catalog &catalog) : SchemaCatalogEntry(catalog, "public", true) {
}

PostgresTransaction &GetPostgresTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<PostgresTransaction>();
}

// FIXME - this is almost entirely copied from TableCatalogEntry::ColumnsToSQL - should be unified
string PostgresColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {
	std::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key columns
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = constraint->Cast<UniqueConstraint>();
			vector<string> constraint_columns = pk.columns;
			if (pk.index.index != DConstants::INVALID_INDEX) {
				// no columns specified: single column constraint
				if (pk.is_primary_key) {
					pk_columns.insert(pk.index);
				} else {
					unique_columns.insert(pk.index);
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after all columns
				if (pk.is_primary_key) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.columns) {
						multi_key_pks.insert(col);
					}
				}
				extra_constraints.push_back(constraint->ToString());
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << KeywordHelper::WriteOptionallyQuoted(column.Name()) << " ";
		ss << PostgresUtils::TypeToString(column.Type());
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
		if (column.Generated()) {
			ss << " GENERATED ALWAYS AS(" << column.GeneratedExpression().ToString() << ")";
		} else if (column.DefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue()->ToString() << ")";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ")";
	return ss.str();
}

string GetCreateTableSQL(CreateTableInfo &info) {
	for (idx_t i = 0; i < info.columns.LogicalColumnCount(); i++) {
		auto &col = info.columns.GetColumnMutable(LogicalIndex(i));
		col.SetType(PostgresUtils::ToPostgresType(col.GetType()));
	}

	std::stringstream ss;
	ss << "CREATE TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	ss << KeywordHelper::WriteOptionallyQuoted(info.table);
	ss << PostgresColumnsToSQL(info.columns, info.constraints);
	ss << ";";
	return ss.str();
}

void PostgresSchemaEntry::TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name) {
	DropInfo info;
	info.type = catalog_type;
	info.name = name;
	info.cascade = false;
	info.if_not_found = OnEntryNotFound::RETURN_NULL;
	DropEntry(context, info);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	auto &base_info = info.Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		TryDropEntry(transaction.GetContext(), CatalogType::TABLE_ENTRY, table_name);
	}

	postgres_transaction.GetConnection().Execute(GetCreateTableSQL(base_info));
	return GetEntry(transaction, CatalogType::TABLE_ENTRY, table_name);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("Postgres databases do not support creating functions");
}

void UnqualifyColumnReferences(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, UnqualifyColumnReferences);
}

string GetCreateIndexSQL(CreateIndexInfo &info, TableCatalogEntry &tbl) {
	string sql;
	sql = "CREATE";
	if (info.constraint_type == IndexConstraintType::UNIQUE) {
		sql += " UNIQUE";
	}
	sql += " INDEX ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.index_name);
	sql += " ON ";
	sql += KeywordHelper::WriteOptionallyQuoted(tbl.name);
	sql += "(";
	for (idx_t i = 0; i < info.parsed_expressions.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		UnqualifyColumnReferences(*info.parsed_expressions[i]);
		sql += info.parsed_expressions[i]->ToString();
	}
	sql += ")";
	return sql;
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateIndex(ClientContext &context, CreateIndexInfo &info,
                                                          TableCatalogEntry &table) {
	auto &postgres_transaction = PostgresTransaction::Get(context, table.catalog);
	postgres_transaction.GetConnection().Execute(GetCreateIndexSQL(info, table));
	return nullptr;
}

string GetCreateViewSQL(CreateViewInfo &info) {
	string sql;
	sql = "CREATE VIEW ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		sql += "IF NOT EXISTS ";
	}
	sql += KeywordHelper::WriteOptionallyQuoted(info.view_name);
	sql += " ";
	if (!info.aliases.empty()) {
		sql += "(";
		for (idx_t i = 0; i < info.aliases.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			auto &alias = info.aliases[i];
			sql += KeywordHelper::WriteOptionallyQuoted(alias);
		}
		sql += ") ";
	}
	sql += "AS ";
	sql += info.query->ToString();
	return sql;
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (info.sql.empty()) {
		throw BinderException("Cannot create view in Postgres that originated from an empty SQL statement");
	}
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		TryDropEntry(transaction.GetContext(), CatalogType::VIEW_ENTRY, info.view_name);
	}
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	postgres_transaction.GetConnection().Execute(GetCreateViewSQL(info));
	return GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("Postgres databases do not support creating sequences");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                  CreateTableFunctionInfo &info) {
	throw BinderException("Postgres databases do not support creating table functions");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                 CreateCopyFunctionInfo &info) {
	throw BinderException("Postgres databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                   CreatePragmaFunctionInfo &info) {
	throw BinderException("Postgres databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                              CreateCollationInfo &info) {
	throw BinderException("Postgres databases do not support creating collations");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Postgres databases do not support creating types");
}

void PostgresSchemaEntry::AlterTable(PostgresTransaction &postgres_transaction, RenameTableInfo &info) {
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " RENAME TO ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_table_name);
	postgres_transaction.GetConnection().Execute(sql);
}

void PostgresSchemaEntry::AlterTable(PostgresTransaction &postgres_transaction, RenameColumnInfo &info) {
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " RENAME COLUMN  ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.old_name);
	sql += " TO ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_name);
	postgres_transaction.GetConnection().Execute(sql);
}

void PostgresSchemaEntry::AlterTable(PostgresTransaction &postgres_transaction, AddColumnInfo &info) {
	if (info.if_column_not_exists) {
		if (postgres_transaction.GetConnection().ColumnExists(info.name, info.new_column.GetName())) {
			return;
		}
	}
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " ADD COLUMN  ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_column.Name());
	sql += " ";
	sql += info.new_column.Type().ToString();
	postgres_transaction.GetConnection().Execute(sql);
}

void PostgresSchemaEntry::AlterTable(PostgresTransaction &postgres_transaction, RemoveColumnInfo &info) {
	if (info.if_column_exists) {
		if (!postgres_transaction.GetConnection().ColumnExists(info.name, info.removed_column)) {
			return;
		}
	}
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " DROP COLUMN  ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.removed_column);
	postgres_transaction.GetConnection().Execute(sql);
}

void PostgresSchemaEntry::Alter(ClientContext &context, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("Only altering tables is supported for now");
	}
	auto &alter = info.Cast<AlterTableInfo>();
	auto &transaction = PostgresTransaction::Get(context, catalog);
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		AlterTable(transaction, alter.Cast<RenameTableInfo>());
		break;
	case AlterTableType::RENAME_COLUMN:
		AlterTable(transaction, alter.Cast<RenameColumnInfo>());
		break;
	case AlterTableType::ADD_COLUMN:
		AlterTable(transaction, alter.Cast<AddColumnInfo>());
		break;
	case AlterTableType::REMOVE_COLUMN:
		AlterTable(transaction, alter.Cast<RemoveColumnInfo>());
		break;
	default:
		throw BinderException("Unsupported ALTER TABLE type - Postgres tables only support RENAME TABLE, RENAME COLUMN, "
		                      "ADD COLUMN and DROP COLUMN");
	}
	transaction.ClearTableEntry(info.name);
}

void PostgresSchemaEntry::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	auto entries = transaction.GetEntries(type, *this);
	for (auto &entry : entries) {
		callback(entry.get());
	}
}
void PostgresSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw InternalException("Scan");
}

void PostgresSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	switch (info.type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
	case CatalogType::INDEX_ENTRY:
		break;
	default:
		throw BinderException("Postgres databases do not support dropping entries of type \"%s\"",
		                      CatalogTypeToString(type));
	}
	auto table = GetEntry(GetCatalogTransaction(context), info.type, info.name);
	if (!table) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw InternalException("Failed to drop entry \"%s\" - could not find entry", info.name);
	}
	auto &transaction = PostgresTransaction::Get(context, catalog);
	transaction.DropEntry(info.type, info.name, info.cascade);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type,
                                                       const string &name) {
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	return postgres_transaction.GetCatalogEntry(type, *this, name);
}

} // namespace duckdb