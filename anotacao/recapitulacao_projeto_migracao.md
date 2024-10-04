# Projeto de Migração de Dados entre Bancos de Dados com Estruturas Diferentes

## Contexto do Projeto

Este projeto tem como objetivo migrar dados entre dois bancos de dados que possuem estruturas diferentes. Os bancos de dados de origem e destino pertencem ao mesmo segmento de vendas, mas possuem tabelas, colunas e arranjos de relacionamento distintos. A migração deve garantir que os dados sejam transferidos corretamente, respeitando as diferenças estruturais entre os bancos.

## Ferramentas e Bibliotecas Utilizadas

- **SQLAlchemy**: Biblioteca ORM (Object-Relational Mapping) para Python, utilizada para mapear as tabelas e colunas dos bancos de dados para objetos Python.
- **SQLAlchemy Automap**: Extensão do SQLAlchemy que permite refletir automaticamente a estrutura do banco de dados em modelos ORM.
- **Pandas**: Biblioteca para manipulação e análise de dados (opcional, dependendo das necessidades de transformação de dados).

## Passos para a Migração de Dados

### 1. Importação das Bibliotecas Necessárias

```python
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
