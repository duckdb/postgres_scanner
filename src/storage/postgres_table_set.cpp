#include "storage/postgres_table_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "storage/postgres_table_entry.hpp"
#include "storage/postgres_view_entry.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parser.hpp"
#include "postgres_conversion.hpp"

namespace duckdb {

PostgresTableSet::PostgresTableSet(PostgresSchemaEntry &schema, unique_ptr<PostgresResultSlice> table_result_p)
    : PostgresInSchemaSet(schema, !table_result_p), table_result(std::move(table_result_p)) {
}

string PostgresTableSet::GetInitializeQuery() {
	return R"(
	SELECT
		pg_namespace.oid,
		relname,
		relkind,
		relpages,
		attname,
		pg_type.typname type_name,
		atttypmod type_modifier,
		pg_attribute.attndims ndim
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
JOIN pg_attribute ON pg_class.oid=pg_attribute.attrelid
JOIN pg_type ON atttypid=pg_type.oid
WHERE attnum > 0 AND relkind IN ('r', 'v', 'm', 'f', 'p')
ORDER BY pg_namespace.oid, relname, attnum;
)";
}

void PostgresTableSet::AddColumn(optional_ptr<PostgresTransaction> transaction,
                                 optional_ptr<PostgresSchemaEntry> schema, PostgresResult &result, idx_t row,
                                 PostgresCreateInfo &pg_create_info, idx_t column_offset) {
	PostgresTypeData type_info;
	idx_t column_index = column_offset;
	auto column_name = result.GetString(row, column_index);
	D_ASSERT(!column_name.empty());
	type_info.type_name = result.GetString(row, column_index + 1);
	type_info.type_modifier = result.GetInt64(row, column_index + 2);
	type_info.array_dimensions = result.GetInt64(row, column_index + 3);
	string default_value;

	PostgresType postgres_type;
	auto column_type = PostgresUtils::TypeToLogicalType(transaction, schema, type_info, postgres_type);
	ColumnDefinition column(column_name, std::move(column_type));
	if (!default_value.empty()) {
		auto expressions = Parser::ParseExpressionList(default_value);
		if (expressions.empty()) {
			throw InternalException("Expression list is empty");
		}
		column.SetDefaultValue(std::move(expressions[0]));
	}
	pg_create_info.AddColumn(std::move(column), std::move(postgres_type), std::move(column_name));
}

static CatalogType TransformRelKind(const string &relkind) {
	if (relkind == "v") {
		return CatalogType::VIEW_ENTRY;
	}
	if (relkind == "r") {
		return CatalogType::TABLE_ENTRY;
	}
	if (relkind == "m") {
		// TODO: support materialized views
		return CatalogType::TABLE_ENTRY;
	}
	if (relkind == "f") {
		// TODO: support foreign tables
		return CatalogType::TABLE_ENTRY;
	}
	if (relkind == "p") {
		// TODO: support partitioned tables
		return CatalogType::TABLE_ENTRY;
	}
	throw InternalException("Unexpected relkind in TransformRelkind: %s", relkind);
}

void PostgresTableSet::CreateEntries(PostgresTransaction &transaction, PostgresResult &result, idx_t start, idx_t end) {
	vector<unique_ptr<PostgresCreateInfo>> infos;
	unique_ptr<PostgresCreateInfo> info;

	for (idx_t row = start; row < end; row++) {
		const idx_t COLUMN_OFFSET = 4;
		auto relname = result.GetString(row, COLUMN_OFFSET - 3);
		D_ASSERT(!relname.empty());
		auto relkind = result.GetString(row, COLUMN_OFFSET - 2);
		auto approx_num_pages = result.GetInt64(row, COLUMN_OFFSET - 1);
		if (!info || info->GetName() != relname) {
			if (info) {
				infos.push_back(std::move(info));
			}
			auto catalog_type = TransformRelKind(relkind);
			switch (catalog_type) {
			case CatalogType::TABLE_ENTRY:
				info = make_uniq<PostgresTableInfo>(schema, relname);
				break;
			case CatalogType::VIEW_ENTRY:
				info = make_uniq<PostgresViewInfo>(schema, relname);
				break;
			default: {
				throw InternalException("Unexpected CatalogType in CreateEntries: %s",
				                        CatalogTypeToString(catalog_type));
			}
				info->approx_num_pages = approx_num_pages;
			}
		}
		AddColumn(&transaction, &schema, result, row, *info, COLUMN_OFFSET);
	}
	if (info) {
		infos.push_back(std::move(info));
	}
	for (auto &pg_create_info : infos) {
		auto &create_info = pg_create_info->GetCreateInfo();
		unique_ptr<StandardEntry> catalog_entry;
		switch (create_info.type) {
		case CatalogType::TABLE_ENTRY:
			catalog_entry = make_uniq<PostgresTableEntry>(catalog, schema, pg_create_info->Cast<PostgresTableInfo>());
			break;
		case CatalogType::VIEW_ENTRY:
			catalog_entry = make_uniq<PostgresViewEntry>(catalog, schema, pg_create_info->Cast<PostgresViewInfo>());
			break;
		default: {
			throw InternalException("Unexpected CatalogType in CreateInfo: %s", CatalogTypeToString(create_info.type));
		}
		}
		CreateEntry(std::move(catalog_entry));
	}
}

void PostgresTableSet::LoadEntries(ClientContext &context) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	if (table_result) {
		CreateEntries(transaction, table_result->GetResult(), table_result->start, table_result->end);
		table_result.reset();
	} else {
		auto query = StringUtil::Replace(R"(
	SELECT 0, relname, relkind, relpages, attname,
		pg_type.typname type_name, atttypmod type_modifier, pg_attribute.attndims ndim
	FROM pg_class
	JOIN pg_namespace ON relnamespace = pg_namespace.oid
	JOIN pg_attribute ON pg_class.oid=pg_attribute.attrelid
	JOIN pg_type ON atttypid=pg_type.oid
	WHERE pg_namespace.nspname=${SCHEMA_NAME} AND attnum > 0 AND relkind IN ('r', 'v', 'm', 'f', 'p')
	ORDER BY relname, attnum;
	)",
		                                 "${SCHEMA_NAME}", KeywordHelper::WriteQuoted(schema.name));

		auto result = transaction.Query(query);
		auto rows = result->Count();

		CreateEntries(transaction, *result, 0, rows);
	}
}

string GetTableInfoQuery(const string &schema_name, const string &table_name) {
	return StringUtil::Replace(StringUtil::Replace(R"(
SELECT
	relpages,
	relkind,
	attname,
	pg_type.typname type_name,
	atttypmod type_modifier,
	pg_attribute.attndims ndim
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
JOIN pg_attribute ON pg_class.oid=pg_attribute.attrelid
JOIN pg_type ON atttypid=pg_type.oid
WHERE pg_namespace.nspname=${SCHEMA_NAME} AND relname=${TABLE_NAME} AND attnum > 0 AND relkind IN ('r', 'v', 'm', 'f', 'p')
ORDER BY relname, attnum;
)",
	                                               "${SCHEMA_NAME}", KeywordHelper::WriteQuoted(schema_name)),
	                           "${TABLE_NAME}", KeywordHelper::WriteQuoted(table_name));
}

unique_ptr<PostgresCreateInfo> PostgresTableSet::GetTableInfo(PostgresTransaction &transaction,
                                                              PostgresSchemaEntry &schema, const string &table_name) {
	auto query = GetTableInfoQuery(schema.name, table_name);
	auto result = transaction.Query(query);
	auto rows = result->Count();
	if (rows == 0) {
		return nullptr;
	}
	const idx_t COLUMN_OFFSET = 2;
	auto relkind = result->GetString(0, 1);
	auto catalog_type = TransformRelKind(relkind);
	unique_ptr<PostgresCreateInfo> info;
	switch (catalog_type) {
	case CatalogType::TABLE_ENTRY:
		info = make_uniq<PostgresTableInfo>(schema, table_name);
		break;
	case CatalogType::VIEW_ENTRY:
		info = make_uniq<PostgresViewInfo>(schema, table_name);
		break;
	default: {
		throw InternalException("Unexpected CatalogType in GetTableInfo: %s", CatalogTypeToString(catalog_type));
	}
	}

	for (idx_t row = 0; row < rows; row++) {
		AddColumn(&transaction, &schema, *result, row, *info, COLUMN_OFFSET);
	}
	info->approx_num_pages = result->GetInt64(0, 0);
	return info;
}

unique_ptr<PostgresCreateInfo> PostgresTableSet::GetTableInfo(PostgresConnection &connection, const string &schema_name,
                                                              const string &table_name) {
	auto query = GetTableInfoQuery(schema_name, table_name);
	auto result = connection.Query(query);
	auto rows = result->Count();
	if (rows == 0) {
		throw InvalidInputException("Table %s does not contain any columns.", table_name);
	}

	const idx_t COLUMN_OFFSET = 2;
	auto relkind = result->GetString(0, 1);
	auto catalog_type = TransformRelKind(relkind);
	unique_ptr<PostgresCreateInfo> info;
	switch (catalog_type) {
	case CatalogType::TABLE_ENTRY:
		info = make_uniq<PostgresTableInfo>(schema_name, table_name);
		break;
	case CatalogType::VIEW_ENTRY:
		info = make_uniq<PostgresViewInfo>(schema_name, table_name);
		break;
	default: {
		throw InternalException("Unexpected CatalogType in GetTableInfo: %s", CatalogTypeToString(catalog_type));
	}
	}
	for (idx_t row = 0; row < rows; row++) {
		AddColumn(nullptr, nullptr, *result, row, *info, COLUMN_OFFSET);
	}
	info->approx_num_pages = result->GetInt64(0, 0);
	return info;
}

optional_ptr<CatalogEntry> PostgresTableSet::ReloadEntry(ClientContext &context, const string &table_name) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	auto pg_info = GetTableInfo(transaction, schema, table_name);
	if (!pg_info) {
		return nullptr;
	}

	unique_ptr<CatalogEntry> entry;
	auto &create_info = pg_info->GetCreateInfo();
	switch (create_info.type) {
	case CatalogType::TABLE_ENTRY: {
		auto &table_info = pg_info->Cast<PostgresTableInfo>();
		entry = make_uniq<PostgresTableEntry>(catalog, schema, table_info);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		auto &view_info = pg_info->Cast<PostgresViewInfo>();
		entry = make_uniq<PostgresViewEntry>(catalog, schema, view_info);
		break;
	}
	default: {
		throw InternalException("Unexpected CatalogType in ReloadEntry: %s", CatalogTypeToString(create_info.type));
	}
	}
	auto entry_ptr = entry.get();
	CreateEntry(std::move(entry));
	return entry_ptr;
}

// FIXME - this is almost entirely copied from TableCatalogEntry::ColumnsToSQL - should be unified
string PostgresColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {
	std::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key
	// columns
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
				// multi-column constraint, this constraint needs to go at the end after
				// all columns
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
		} else if (column.HasDefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue().ToString() << ")";
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

string GetPostgresCreateTable(CreateTableInfo &info) {
	for (idx_t i = 0; i < info.columns.LogicalColumnCount(); i++) {
		auto &col = info.columns.GetColumnMutable(LogicalIndex(i));
		col.SetType(PostgresUtils::ToPostgresType(col.GetType()));
	}

	std::stringstream ss;
	ss << "CREATE TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	if (!info.schema.empty()) {
		ss << KeywordHelper::WriteQuoted(info.schema, '"');
		ss << ".";
	}
	ss << KeywordHelper::WriteQuoted(info.table, '"');
	ss << PostgresColumnsToSQL(info.columns, info.constraints);
	ss << ";";
	return ss.str();
}

optional_ptr<CatalogEntry> PostgresTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	auto create_sql = GetPostgresCreateTable(info.Base());
	transaction.Query(create_sql);
	auto tbl_entry = make_uniq<PostgresTableEntry>(catalog, schema, info.Base());
	return CreateEntry(std::move(tbl_entry));
}

void PostgresTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(schema.name) + ".";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " RENAME TO ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_table_name);
	transaction.Query(sql);
}

void PostgresTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(schema.name) + ".";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " RENAME COLUMN  ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.old_name);
	sql += " TO ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_name);

	transaction.Query(sql);
}

void PostgresTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(schema.name) + ".";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " ADD COLUMN  ";
	if (info.if_column_not_exists) {
		sql += "IF NOT EXISTS ";
	}
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_column.Name());
	sql += " ";
	sql += info.new_column.Type().ToString();
	transaction.Query(sql);
}

void PostgresTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(schema.name) + ".";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " DROP COLUMN  ";
	if (info.if_column_exists) {
		sql += "IF EXISTS ";
	}
	sql += KeywordHelper::WriteOptionallyQuoted(info.removed_column);
	transaction.Query(sql);
}

void PostgresTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		AlterTable(context, alter.Cast<RenameTableInfo>());
		break;
	case AlterTableType::RENAME_COLUMN:
		AlterTable(context, alter.Cast<RenameColumnInfo>());
		break;
	case AlterTableType::ADD_COLUMN:
		AlterTable(context, alter.Cast<AddColumnInfo>());
		break;
	case AlterTableType::REMOVE_COLUMN:
		AlterTable(context, alter.Cast<RemoveColumnInfo>());
		break;
	default:
		throw BinderException("Unsupported ALTER TABLE type - Postgres tables only "
		                      "support RENAME TABLE, RENAME COLUMN, "
		                      "ADD COLUMN and DROP COLUMN");
	}
	ClearEntries();
}

} // namespace duckdb
