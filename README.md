# CantuStore-marcelodb
## Prova Equipe de Dados - Resultados

## CantuStore

Somos a **CantuStore**: uma plataforma de tecnologia e logística que oferece soluções completas em pneus, guiando quem compra e apoiando quem vende. Se o assunto é pneu, você resolve aqui. Produtos e serviços em uma experiência 360° para abrir caminhos e ver pessoas e negócios evoluindo junto com a gente. Afinal, ficar parado não é opção, pelo menos para nós.

### Parte 1 - SQL

#### 1.1 Campeonato

A organização e os resultados de um campeonato estão estruturados conforme abaixo:

**Tabela Times:**
- `time_id` INTEGER NOT NULL
- `time_nome` VARCHAR NOT NULL

**Tabela Jogos:**
- `jogo_id` INTEGER NOT NULL
- `mandante_time` INTEGER NOT NULL
- `visitante_time` INTEGER NOT NULL
- `mandante_gols` INTEGER NOT NULL
- `visitante_gols` INTEGER NOT NULL

Regras para o cálculo dos pontos:
- Vitória: 3 pontos.
- Empate: 1 ponto.
- Derrota: 0 pontos.

**Resultado da Classificação:**

| Time       | Pontos |
|------------|--------|
| Marketing  | 7      |
| Dados      | 4      |
| Financeiro | 3      |
| Logística  | 3      |
| TI         | 2      |

#### 1.2 Comissões

**Estrutura da Tabela Comissões:**

- `comprador` VARCHAR NOT NULL
- `vendedor` VARCHAR NOT NULL
- `dataPgto` DATE NOT NULL
- `valor` FLOAT NOT NULL

**Resultado:**

| Vendedor | Total de Transferências |
|----------|-------------------------|
| Lucas    | 3                       |
| Matheus  | 1                       |

#### 1.3 Organização Empresarial

**Estrutura da Tabela de Colaboradores:**

- `id` INTEGER NOT NULL
- `nome` VARCHAR NOT NULL
- `salario` INTEGER NOT NULL
- `lider_id` INTEGER NOT NULL

**Resultado:**

| Funcionário | Chefe Direto |
|-------------|--------------|
| Helen       | Bruno        |
| Bruno       | Leonardo     |
| Leonardo    | Marcos       |
| Marcos      | NULL         |
| Mateus      | Leonardo     |
| Cinthia     | NULL         |
| Wilian      | NULL         |

## Como Utilizar

1. **Campeonato**:
   - Resultados e pontuações do campeonato de acordo com a tabela e regras especificadas.

2. **Comissões**:
   - Identifica os vendedores que receberam até 1024 reais em até três transferências.

3. **Organização Empresarial**:
   - Exibe a hierarquia da empresa, identificando os chefes diretos e indiretos.

### Parte 2 - Análise de Dados

Um dos problemas mais clássicos do mercado de e-commerce é o carrinho abandonado. Carrinho abandonado é aquele em que o cliente seleciona produtos para compra no e-commerce, mas não finaliza o processo. Algumas das estratégias para recuperá-lo são investir em remarketing, flexibilizar o frete e melhorar o checkout. Este projeto visa ajudar a Cantu a coletar informações relacionadas a esse evento.

#### Perguntas Respondidas:

1. **Quais os produtos que mais tiveram carrinhos abandonados?**
   - Resultado: [Dados disponíveis no arquivo HTML de análise](#).

2. **Quais as duplas de produtos em conjunto que mais tiveram carrinhos abandonados?**
   - Resultado: [Dados disponíveis no arquivo HTML de análise](#).

3. **Quais produtos tiveram um aumento de abandono?**
   - Resultado: [Dados disponíveis no arquivo HTML de análise](#).

4. **Quais os produtos novos e a quantidade de carrinhos no seu primeiro mês de lançamento?**
   - Resultado: [Dados disponíveis no arquivo HTML de análise](#).

5. **Quais estados tiveram mais abandonos?**
   - Resultado: [Dados disponíveis no arquivo HTML de análise](#).

#### Relatórios Gerados:

1. **Relatório Mensal por Produto:**
   - Quantidade de carrinhos abandonados, quantidade de itens abandonados e o valor não faturado.
   - Resultado: [Dados disponíveis no arquivo HTML de análise](#).

2. **Relatório Diário:**
   - Quantidade de carrinhos abandonados, quantidade de itens abandonados e o valor não faturado.
   - Resultado: [Dados disponíveis no arquivo HTML de análise](#).

#### Arquivo de Exportação:

- Arquivo `.txt` com os 50 carrinhos com os maiores valores (`carts.p_totalprice`), conforme o layout especificado.
- Detalhes sobre o arquivo exportado estão disponíveis [aqui](#).
