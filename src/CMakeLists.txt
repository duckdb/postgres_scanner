include_directories(include)

add_subdirectory(storage)

add_library(
  postgres_ext_library OBJECT
  postgres_connection.cpp
  postgres_extension.cpp
  postgres_scanner.cpp
  postgres_stmt.cpp
  postgres_storage.cpp
  postgres_utils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:postgres_ext_library>
    PARENT_SCOPE)
