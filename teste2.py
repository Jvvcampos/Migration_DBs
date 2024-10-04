import fdb

# Conexão com os bancos de dados
con_destino = fdb.connect(
    dsn='C:\\Users\\admin\\Desktop\\WINSAE.GDB',
    user='SYSDBA',
    password='masterkey'
)
con_origem = fdb.connect(
    dsn='C:\\winsae\\DVERAS',
    user='SYSDBA',
    password='masterkey'
)

# Criação de cursores
cur_origem = con_origem.cursor()
cur_destino = con_destino.cursor()

# Mapeamento de tabelas e colunas com generators
mapa_tabelas = {
    'CORES': {
        'PROD01_COR': {
            'tabela_destino': 'PROD01_COR',
            'colunas': {
                'DESCRICAO': 'NOME_COR'
            },
            'generator': 'GEN_PROD01_COR_ID'
        },
        'NCM': {
            'tabela_destino': 'NCM',
            'colunas': {
                'NCM': 'COD_NCM'
            }
        }
    }
    # Adicione mais mapeamentos conforme necessário
}
""",
    'OUTRA_TABELA': {
        'tabela_destino': 'NOME_TABELA_DESTINO',
        'colunas': {
            'COLUNA1' : 'COD_COLUNA1',
            'COLUNA2': 'COD_COLUNA2'
        },
        'generator': 'GEN_COD_OUTRA_TABELA'  # Generator específico para a outra tabela
    }"""

def gerar_codigo_firebird(generator_name):
    cur_destino.execute(f"SELECT GEN_ID({generator_name}, 1) FROM RDB$DATABASE")
    return cur_destino.fetchone()[0]
    
def migrar_dados(tabela_origem_nome, tabelas_destino, colunas_mapeamento, generator_name=None):
    # Extração de dados da tabela de origem
    cur_origem.execute(f"SELECT {', '.join(colunas_mapeamento.keys())} FROM {tabela_origem_nome}")
    dados_origem = cur_origem.fetchall()

    for tabela_destino_nome in tabelas_destino:
        for dado in dados_origem:
        
            # Criação da query de inserção
            colunas_destino = ', '.join(colunas_mapeamento.values())
            valores_destino = ', '.join(['?'] * len(colunas_mapeamento))
        
            # Execução da query de inserção
            if generator_name: # Gerar código Firebird usando o generator específico
                query = f"INSERT INTO {tabela_destino_nome} ({colunas_destino}, COD_COR) VALUES ({valores_destino}, ?)"
                codigo = gerar_codigo_firebird(generator_name)
                cur_destino.execute(query, list(dado) + [codigo])
            else:
                query = f"INSERT INTO {tabela_destino_nome} ({colunas_destino}) VALUES ({valores_destino})"
                cur_destino.execute(query, list(dado))
            
    # Commit das transações
    con_destino.commit()

# Executar a migração para cada tabela mapeada
for tabela_origem, mapeamento in mapa_tabelas.items():
    generator_name = mapeamento.get('generator')  # Obtém o valor do generator ou None se não existir
    tabelas_destino = [mapeamento['tabela_destino']] if isinstance(mapeamento['tabela_destino'], str) else mapeamento['tabela_destino']

    migrar_dados(tabela_origem, tabelas_destino, mapeamento['colunas'], generator_name)

# Fechar conexões
con_origem.close()
con_destino.close()
