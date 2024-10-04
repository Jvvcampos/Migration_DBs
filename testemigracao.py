from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

# Conexão com os bancos de dados
engine_origem = create_engine('mysql+pymysql://usuario:senha@host:porta/banco_origem')
engine_destino = create_engine('postgresql://usuario:senha@host:porta/banco_destino')

# Refletir a estrutura do banco de dados de origem
BaseOrigem = automap_base()
BaseOrigem.prepare(engine_origem, reflect=True)

# Refletir a estrutura do banco de dados de destino
BaseDestino = automap_base()
BaseDestino.prepare(engine_destino, reflect=True)

# Sessões
SessionOrigem = sessionmaker(bind=engine_origem)
SessionDestino = sessionmaker(bind=engine_destino)
session_origem = SessionOrigem()
session_destino = SessionDestino()

# Mapeamento de tabelas e colunas
mapa_tabelas = {
    'tabela_origem1': {
        'tabela_destino': 'tabela_destino1',
        'colunas': {
            'id': 'id',
            'nome': 'nome_completo',
            'valor': 'preco'
        }
    },
    'tabela_origem2': {
        'tabela_destino': 'tabela_destino2',
        'colunas': {
            'codigo': 'codigo_produto',
            'descricao': 'descricao_produto',
            'quantidade': 'quantidade_estoque'
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
    
    