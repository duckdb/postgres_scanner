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
	ss << TableCatalogEntry::ColumnsToSQL(info.columns, info.constraints);
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
	vector<string> entries;
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		entries = transaction.GetConnection().GetTables(name);
		break;
	case CatalogType::VIEW_ENTRY:
		entries = transaction.GetConnection().GetEntries("view");
		break;
	case CatalogType::INDEX_ENTRY:
		entries = transaction.GetConnection().GetEntries("index");
		break;
	default:
		// no entries of this catalog type
		return;
	}
	for (auto &entry_name : entries) {
		callback(*GetEntry(GetCatalogTransaction(context), type, entry_name));
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