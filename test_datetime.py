import duckdb
import pandas as pd
try:
    con = duckdb.connect()
    val_carga = con.execute("SELECT MIN(data_carga) FROM 'agregado_detalhado_por_convenio_ano.parquet'").fetchone()[0]
    print(repr(val_carga))
    print(type(val_carga))
    data_limpa = pd.to_datetime(val_carga).strftime('%d/%m/%Y')
    print("Sucesso:", data_limpa)
except Exception as e:
    import traceback
    traceback.print_exc()
