import sqlalchemy_firebird.fdb as fdb
from sqlalchemy import create_engine, MetaData


con = fdb.connect(
    dsn='C:\\Users\\admin\\Desktop\\WINSAE.GDB',
    user='SYSDBA',
    password='masterkey'
)
print("Conexão bem-sucedida!")
con.close()

engine_destino = create_engine('firebird+fdb://SYSDBA:masterkey@127.0.0.1:3050/C:\\Users\\admin\\Desktop\\WINSAE.GDB')

    colunas_sql = ', '.join([col for col in dado_transformado.keys() if col != 'COD_COR'])
    query = f"INSERT INTO {tabela_destino_nome} ({colunas_sql}) VALUES ({valores_sql})"

    $sqlGen = " select GEN_ID(GEN_CAD_FOTO_ID, 1) FROM RDB\$DATABASE ";
            $query = $b->query($sqlGen);
            $codigoID = $query->row();
            $cod = ($codigoID->GEN_ID);
