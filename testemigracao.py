from sqlalchemy import create_engine, MetaData
import sqlalchemy_firebird.fdb as fdb
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.dialects import registry
from sqlalchemy.orm import sessionmaker

registry.register("firebird.fdb", "sqlalchemy_firebird.fdb", "FBDialect_fdb")

# Conexão com os bancos de dados
#db_uri = "firebird+firebird://sysdba@/c:/projects/databases/my_project.fdb?charset=UTF8&fb_client_library=c:/projects/databases/fb40_svr/fbclient.dll"
engine_origem = create_engine('firebird+fdb://127.0.0.1:3050/C:\\winsae\\DVERAS')
engine_destino = create_engine('firebird+fdb://SYSDBA:masterkey@127.0.0.1:3050/C:\\Users\\admin\\Desktop\\WINSAE.GDB')
# Refletir a estrutura do banco de dados de origem
BaseOrigem = automap_base()
BaseOrigem.prepare(autoload_with=engine_origem)

# Refletir a estrutura do banco de dados de destino
BaseDestino = automap_base()
BaseDestino.prepare(autoload_with=engine_destino)

# Sessões
SessionOrigem = sessionmaker(bind=engine_origem)
SessionDestino = sessionmaker(bind=engine_destino)
session_origem = SessionOrigem()
session_destino = SessionDestino()

# Mapeamento de tabelas e colunas
mapa_tabelas = {
    'CORES': {
        'tabela_destino': 'PROD01_COR',
        'colunas': {
            'COR': 'COD_COR',
            'DESCRICAO': 'NOME_COR'
        }
    }
    # Adicione mais mapeamentos conforme necessário
}

def migrar_dados(tabela_origem_nome, tabela_destino_nome, colunas_mapeamento):
    # Obter as classes mapeadas para as tabelas de origem e destino
    TabelaOrigem = getattr(BaseOrigem.classes, tabela_origem_nome)
    TabelaDestino = getattr(BaseDestino.classes, tabela_destino_nome)

    # Extração de dados da tabela de origem
    dados_origem = session_origem.query(TabelaOrigem).all()

    for dado in dados_origem:
        # Transformação personalizada dos dados
        dado_transformado = {col_destino: getattr(dado, col_origem) for col_origem, col_destino in colunas_mapeamento.items()}
        
        # Criação de um novo objeto para a tabela de destino
        novo_dado = TabelaDestino(**dado_transformado)
        
        # Adiciona o novo registro à sessão do banco de dados de destino
        session_destino.add(novo_dado)

    # Commit das transações
    session_destino.commit()

for tabela_origem, mapeamento in mapa_tabelas.items():
    migrar_dados(tabela_origem, mapeamento['tabela_destino'], mapeamento['colunas'])    
    
    