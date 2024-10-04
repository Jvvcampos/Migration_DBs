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

# Mapeamento de tabelas e colunas
mapa_tabelas = {
    'CORES': {
        'tabela_destino': 'PROD01_COR',
        'colunas': {
            'COR' : 'COD_COR',
            'DESCRICAO': 'NOME_COR'
        }
    }
    # Adicione mais mapeamentos conforme necessário
}

def migrar_dados(tabela_origem_nome, tabela_destino_nome, colunas_mapeamento):
    # Extração de dados da tabela de origem
    cur_origem.execute(f"SELECT {', '.join(colunas_mapeamento.keys())} FROM {tabela_origem_nome}")
    dados_origem = cur_origem.fetchall()

    for dado in dados_origem:
        # Transformação personalizada dos dados
        if None in dado:
            print("Registro ignorado devido a valores nulos.")
            continue
        dado_transformado = {col_destino: dado[idx] if dado[idx] is not None else 'default_value' for idx, col_destino in enumerate(colunas_mapeamento.values())}
        
        # Verificação de valores nulos
        if 'default_value' in dado_transformado.values():
            print(f"Registro ignorado devido a valores nulos: {dado_transformado}")
            continue  # Ignora o registro ou trate conforme necessário
        
        # Criação da query de inserção
        colunas_destino = ', '.join(dado_transformado.keys())
        valores_destino = ', '.join(['?'] * len(dado_transformado))
        query = f"INSERT INTO {tabela_destino_nome} ({colunas_destino}) VALUES ({valores_destino})"
        
        # Execução da query de inserção
        cur_destino.execute(query, list(dado_transformado.values()))
        #cur_destino.execute(query)

    # Commit das transações
    con_destino.commit()

# Executar a migração para cada tabela mapeada
for tabela_origem, mapeamento in mapa_tabelas.items():
    
    migrar_dados(tabela_origem, mapeamento['tabela_destino'], mapeamento['colunas'])

# Fechar conexões
con_origem.close()
con_destino.close()
