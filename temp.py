import duckdb
con = duckdb.connect()
print(con.execute("SELECT typeof(data_carga), data_carga FROM 'agregado_detalhado_por_convenio_ano.parquet' LIMIT 2").fetchall())
