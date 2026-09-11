set(POSTGRES_DBCONNECTOR_PATH
    "${CMAKE_CURRENT_LIST_DIR}/../../database-connector")

add_library(
  postgres_ext_dbconnector OBJECT
  ${POSTGRES_DBCONNECTOR_PATH}/src/attached/attached_catalog.cpp
  ${POSTGRES_DBCONNECTOR_PATH}/src/attached/attached_table.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:postgres_ext_dbconnector>
    PARENT_SCOPE)
