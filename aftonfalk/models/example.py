from types_ import Table

example = Table(
    source_path="cha.raw.dimencounter",
    destination_path="cha.clean.dimencounter",
    data_modified_field_enabled=True,
    source_data_modified_column_name="UpdatedAt"
    )

example.read_sql()

example.read_sql(incremental=True, since="2023-10-02T15:45:30", until="2023-11-02T15:45:30")

example.table_ddl()