# TestTecnicoIndicium

## Descrição
Este projeto foi desenvolvido como parte de um desafio técnico e tem como objetivo processar e analisar dados da empresa Northwind. Ele inclui a extração, transformação e carga de dados (ETL), utilizando `dbt` para modelagem de dados.

## Estrutura do Projeto
```
TestTecnicoIndicium/
|─ .venv/                   # Ambiente virtual Python
|─ data/                    # Scripts de extração e carga
|   |─ extract.py
|   |─ load.py
|─ db/                      # Base de dados local (se aplicável)
|─ logs/                    # Logs de execução
|─ northwind_dbt/           # Diretório dbt para modelagem de dados
|   |─ dbt_packages/        # Pacotes dbt instalados
|   |─ models/             # Modelos dbt
|       |─ analytics/      # Análises
|       |─ staging/       # Camada staging
|─ target/                  # Saída do dbt
|─ tests/                   # Testes automatizados
|─ .gitignore                # Arquivos a serem ignorados pelo Git
|─ dbt_project.yml           # Configuração do projeto dbt
|─ profiles.yml              # Perfil de conexão dbt
|─ main.py                   # Script principal
|─ requirements.txt           # Dependências do projeto
|─ README.md                 # Documentação do projeto
```

## Requisitos
Antes de rodar o projeto, é necessário instalar algumas dependências. As principais bibliotecas são:

*(Atualizar com bibliotecas do requirements.txt)*

## Instalação
1. Clone este repositório:
   ```sh
   git clone https://github.com/seu-usuario/TestTecnicoIndicium.git
   cd TestTecnicoIndicium
   ```
2. Crie e ative um ambiente virtual:
   ```sh
   python -m venv .venv
   source .venv/bin/activate  # Linux/macOS
   .venv\Scripts\activate     # Windows
   ```
3. Instale as dependências:
   ```sh
   pip install -r requirements.txt
   ```

## Como Rodar o Projeto
Para rodar o pipeline de ETL:
```sh
python main.py
```

Para rodar os modelos dbt:
```sh
dbt run
```

Para testar os modelos dbt:
```sh
dbt test
```

## Configuração do dbt
Antes de rodar o `dbt`, configure o arquivo `profiles.yml` corretamente com as credenciais do banco de dados.



