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

# Definição dos mapeamentos de origem para destino
mapeamentos = [
    {
        'origem': 'ORIGEM1',
        'destinos': [
            {
                'tabela_destino': 'TABELA_DESTINO1',
                'colunas': ['COLUNA1', 'COLUNA2']
            },
            {
                'tabela_destino': 'TABELA_DESTINO2',
                'colunas': ['COLUNA3', 'COLUNA4']
            }
        ]
    },
    {
        'origem': 'ORIGEM2',
        'destinos': [
            {
                'tabela_destino': 'TABELA_DESTINO1',
                'colunas': ['COLUNA1', 'COLUNA2']
            },
            {
                'tabela_destino': 'TABELA_DESTINO3',
                'colunas': ['COLUNA5', 'COLUNA6']
            }
        ]
    }
    # Adicione mais mapeamentos conforme necessário
]

# Criação de cursores
cur_origem = con_origem.cursor()
cur_destino = con_destino.cursor()

# Função para atualizar registros existentes na tabela de destino
def atualizar_registros(tabela_destino, colunas_atualizacao, dados_atualizacao):
    query_update = f"UPDATE {tabela_destino} SET {', '.join([f'{col} = ?' for col in colunas_atualizacao])} WHERE <condição_de_identificação>"
    cur_destino.execute(query_update, dados_atualizacao)

# Função para inserir novos registros na tabela de destino
def inserir_registros(tabela_destino, colunas_destino, valores_destino):
    query_insert = f"INSERT INTO {tabela_destino} ({', '.join(colunas_destino)}) VALUES ({', '.join(['?'] * len(colunas_destino))})"
    cur_destino.execute(query_insert, valores_destino)
    
def dados_existem_na_tabela_destino(tabela_destino, condicao_identificacao):
    query_check = f"SELECT COUNT(1) FROM {tabela_destino} WHERE {condicao_identificacao}"
    cur_destino.execute(query_check)
    return cur_destino.fetchone()[0] > 0

# Função para migrar dados de uma origem para múltiplos destinos
def migrar_dados(origem, destinos):
    for destino in destinos:
        colunas_origem = ', '.join(destino['colunas'])
        cur_origem.execute(f"SELECT {colunas_origem} FROM {origem}")
        dados_origem = cur_origem.fetchall()

   
        for dado in dados_origem:
            # Transformação dos dados, se necessário
            dados_insercao = list(dado)

            # Verificar se os dados já existem na tabela de destino
            condicao_identificacao = "<condição_de_identificação>"  # Defina a condição de identificação
            if dados_existem_na_tabela_destino(destino['tabela_destino'], condicao_identificacao):
                atualizar_registros(destino['tabela_destino'], destino['colunas'], dados_insercao, condicao_identificacao)
            else:
                inserir_registros(destino['tabela_destino'], destino['colunas'], dados_insercao)
    con_destino.commit()

# Executar a migração de dados para cada mapeamento
for mapeamento in mapeamentos:
    migrar_dados(mapeamento['origem'], [dest['tabela_destino'] for dest in mapeamento['destinos']], mapeamento['destinos'][0]['colunas'])

con_origem.close()
con_destino.close()
