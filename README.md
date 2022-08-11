### Aula 05 - Ingestão de Dados - PECE/USP

#### Uso de Serverless para processar as rotinas em Python criadas na Aula 02.

<img width="916" alt="Captura de Tela 2022-08-11 às 19 07 08" src="https://user-images.githubusercontent.com/9318332/184250728-1dbcc167-6e26-49be-b0ca-fc37edc0094f.png">

### Workflow

1) Criamos o Bucket RAW e carregamos os CSVs

2) Criamos o Bucket Trusted

3) Criamos o banco de dados MySQL no RDS

4) Criamos os JOBs que tratam os dados e salvam os dimensionamentos e fatos em “Trusted”

5) Os dados gerados pelos JOBs do Amazon Glue foram armazenados no MySQL

## Referência
 - [Ranking de Instituições por Índice de Reclamações](https://dados.gov.br/dataset/ranking-de-instituicoes-por-indice-de-reclamacoes)
 - [ Tarifas Bancárias - por Segmento e por Instituição](https://dados.gov.br/dataset/tarifas-bancarias-por-segmento-e-por-instituicao)
 - [AWS Glue](https://aws.amazon.com/pt/glue/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)
## Autores

- [@erikassuncao](https://www.github.com/erikassuncao)
- Jorge Alexandre Pires de Oliveira
- Paulo Henrique de Souza Pereira Prazeres
- Willian Alves Barboza
