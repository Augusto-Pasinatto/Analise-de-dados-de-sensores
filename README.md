# Análise de Dados dos Sensores - CityLivingLab

Projeto desenvolvido em **C** para a disciplina de **Fundamentos de Sistemas Operacionais**.

O programa realiza o processamento de arquivos JSON contendo medições de sensores instalados nas cidades de **Caxias do Sul** e **Bento Gonçalves**, utilizando **pthreads** para separar as etapas de leitura, cálculo de estatísticas e registro de logs.

## Objetivo

O objetivo do projeto é analisar dados coletados por sensores do projeto **CityLivingLab**, calculando estatísticas relacionadas a:

- Temperatura
- Umidade
- Pressão atmosférica
- Nível de bateria
- Spreading Factors, quando disponíveis

Além disso, o programa remove registros duplicados, trata dados ausentes e registra logs para auditoria da execução.

## Tecnologias utilizadas

- Linguagem C
- pthreads
- cJSON
- Linux / Xubuntu
- GCC

## Funcionalidades

O programa realiza as seguintes operações:

- Leitura de arquivos JSON
- Suporte a JSON com os campos `brute_data` ou `payload`
- Extração de dados internos dos sensores
- Identificação da cidade pelo campo `device_name`
- Remoção de registros duplicados
- Tratamento de registros incompletos ou inválidos
- Cálculo de estatísticas por cidade
- Geração de arquivo de log
- Medição do tempo total de execução
- Exibição dos resultados em formato tabular no terminal

## Estatísticas calculadas

Para cada cidade, o programa calcula:

- Menor temperatura registrada
- Maior temperatura registrada
- Média de temperatura
- Menor umidade registrada
- Maior umidade registrada
- Média de umidade
- Menor pressão atmosférica registrada
- Maior pressão atmosférica registrada
- Média de pressão atmosférica
- Bateria inicial
- Bateria final
- Consumo de bateria
- Spreading Factors utilizados, quando informados

## Organização das threads

O programa utiliza 3 threads principais:

| Thread | Função |
|---|---|
| Thread 1 | Leitura dos dados e eliminação de duplicatas |
| Thread 2 | Cálculo das estatísticas |
| Thread 3 | Registro das mensagens de log |

A sincronização entre as threads é feita com `mutex` e `condition variables`.

## Arquivos esperados

Por padrão, o programa tenta ler os seguintes arquivos:

```bash
sensores_caxias.json
sensores_bento.json
