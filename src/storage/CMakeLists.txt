add_library(
  postgres_ext_storage OBJECT
  postgres_catalog.cpp
  postgres_catalog_set.cpp
  postgres_delete.cpp
  postgres_insert.cpp
  postgres_schema_entry.cpp
  postgres_schema_set.cpp
  postgres_table_entry.cpp
  postgres_table_set.cpp
  postgres_transaction.cpp
  postgres_transaction_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:postgres_ext_storage>
    PARENT_SCOPE)
