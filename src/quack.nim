{.passl: "-lduckdb"}

type
  duckdb_state = enum
    DuckDBSuccess = 0,
    DuckDBError = 1

  duckdb_database = object
    internal_ptr: pointer

  duckdb_connection = object
    internal_ptr: pointer

  duckdb_result = object
    internal_ptr: pointer

  duckdb_data_chunk = object
    internal_ptr: pointer

  duckdb_vector = object
    internal_ptr: pointer


proc duckdb_open(path: cstring, db: ptr duckdb_database): duckdb_state                               {.cdecl, importc.}
proc duckdb_connect(db: duckdb_database, conn: ptr duckdb_connection): duckdb_state                  {.cdecl, importc.}
proc duckdb_disconnect(conn: ptr duckdb_connection)                                                  {.cdecl, importc.}
proc duckdb_close(db: ptr duckdb_database)                                                           {.cdecl, importc.}

proc duckdb_query(conn: duckdb_connection, query: cstring, result: ptr duckdb_result): duckdb_state  {.cdecl, importc.}
proc duckdb_fetch_chunk(result: duckdb_result): duckdb_data_chunk                                    {.cdecl, importc.}
proc duckdb_data_chunk_get_size(result: duckdb_data_chunk): uint64                                   {.cdecl, importc.}
proc duckdb_data_chunk_get_vector(result: duckdb_data_chunk, col_idx: uint64): duckdb_vector         {.cdecl, importc.}

proc duckdb_vector_get_data(col: duckdb_vector): ptr UncheckedArray[int32]                           {.cdecl, importc.}
proc duckdb_vector_get_validity(col: duckdb_vector): ptr uint64                                      {.cdecl, importc.}
proc duckdb_validity_row_is_valid(col_validity: ptr uint64, row_idx: uint64): bool                   {.cdecl, importc.}

proc duckdb_destroy_data_chunk(result: ptr duckdb_data_chunk)                                        {.cdecl, importc.}


when isMainModule:
  var state: duckdb_state
  var db = duckdb_database()
  var conn = duckdb_connection()
  var query_result = duckdb_result()
  if duckdb_open(nil, db.addr) == DuckDBError:
    echo "Failed to open database."
  if duckdb_connect(db, conn.addr) == DuckDBError:
    echo "Failed to close database."

  state = duckdb_query(conn, "CREATE TABLE integers (i INTEGER, j INTEGER);".cstring, nil);
  if (state == DuckDBError):
      echo "create table failed"

  state = duckdb_query(conn, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);".cstring, nil);
  if (state == DuckDBError):
      echo "insert failed"

  state = duckdb_query(conn, "SELECT * FROM integers".cstring, query_result.addr);
  if (state == DuckDBError):
      echo "select failed"

  while true:
    var chunk: duckdb_data_chunk
    chunk = duckdb_fetch_chunk(query_result)  # this segfaults for some reason

    if result.internal_ptr == nil:
      break

    # get the number of rows from the data chunk
    let row_count = duckdb_data_chunk_get_size(chunk)
    # get the first column
    let col1 = duckdb_data_chunk_get_vector(chunk, 0)
    let col1_data = duckdb_vector_get_data(col1)
    let col1_validity = duckdb_vector_get_validity(col1)

    # get the second column
    let col2 = duckdb_data_chunk_get_vector(chunk, 1)
    let col2_data = duckdb_vector_get_data(col2)
    let col2_validity = duckdb_vector_get_validity(col2)

    # iterate over the rows
    for row in 0 ..< row_count:
      if duckdb_validity_row_is_valid(col1_validity, row):
        echo col1_data[row]
      else:
        echo "NULL"

      echo ","
      if duckdb_validity_row_is_valid(col2_validity, row):
        echo col2_data[row]
      else:
          echo "NULL"
      echo "\n"

      duckdb_destroy_data_chunk(result.addr)

  duckdb_disconnect(conn.addr)
  duckdb_close(db.addr)
