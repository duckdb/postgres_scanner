include_directories(include)

add_library(
  postgres_ext_library OBJECT
  postgres_extension.cpp
  postgres_scanner.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:postgres_ext_library>
    PARENT_SCOPE)
