# Automação Check-in AP — Documentação Técnica

## Visão Geral

Script Python que automatiza o processamento da planilha **CheckinAP**, substituindo todo o trabalho manual de fórmulas (PROCV, condicionais, concatenações) na aba "AP by GL" por processamento programático via **pandas**.

---

## Arquitetura do Processo

```
┌─────────────────────┐     ┌──────────────────────┐
│  Bases do ERP (RLM) │     │  Tabelas de Referência│
│  (1 arquivo/company)│     │  (Currency, Kyriba,   │
│  Colunas A até AC   │     │   Transfer Type,      │
│                     │     │   Validação)           │
└────────┬────────────┘     └──────────┬───────────┘
         │                              │
         ▼                              ▼
┌─────────────────────────────────────────────────┐
│           automate_checkin_ap.py                 │
│                                                  │
│  1. Consolida bases (remove subtotais)           │
│  2. Carrega tabelas de referência (lookups)      │
│  3. Aplica 17 transformações (colunas AD+)       │
│  4. Gera PYM LIST e Resumo                       │
│  5. Valida dados (alertas de inconsistência)     │
│  6. Exporta Excel formatado                      │
└────────────────────┬────────────────────────────┘
                     ▼
         ┌───────────────────────┐
         │ CheckinAP_Automatizado│
         │ .xlsx                 │
         │                      │
         │ • AP By GL (completa) │
         │ • PYM LIST            │
         │ • Resumo              │
         │ • Tabelas referência  │
         └───────────────────────┘
```

---

## Instalação

```bash
pip install pandas openpyxl numpy
```

---

## Modos de Uso

### Modo 1 — Consolidar múltiplas bases do ERP

Quando você tem vários arquivos (um por company) na pasta `Bases`:

```bash
python automate_checkin_ap.py --bases "M:\FARM Global\...\Bases" --ref CheckinAP_Ref.xlsx
```

O script vai:
- Ler todos os `.xlsx` da pasta
- Remover linhas de subtotal/total (coluna A em branco)
- Consolidar em um único DataFrame

### Modo 2 — Processar arquivo único

Quando o arquivo já está consolidado (como o `Check_in_AP_-_260318.xlsx`):

```bash
python automate_checkin_ap.py --input Check_in_AP_-_260318.xlsx
```

### Opções adicionais

```bash
--output NOME.xlsx    # Nome personalizado para o arquivo de saída
```

---

## Mapeamento Completo das Transformações

Cada coluna calculada (AD em diante) e sua lógica implementada:

| Coluna | Lógica | Fonte |
|--------|--------|-------|
| **Original Amount** | `Amount($) × Exchange Rate` | Colunas AB × AA |
| **Currency** | PROCV `Vendor Curr` → aba Currency | Aba Currency |
| **Vendor Bank Country** | PROCV `Vendor #` → Kyriba (C.A.) coluna Country | Aba Kyriba (C.A.) |
| **Credit Account (Kyriba)** | PROCV `Vendor #` → Kyriba (C.A.) coluna Code | Aba Kyriba (C.A.) |
| **Código** | Concatenação: `Company#` + `Currency` + `Country` | Calculado |
| **Debit Account (Kyriba)** | PROCV `Código` → Transfer Type coluna Kyriba Debit Account | Aba Transfer Type |
| **Transfer Type** | PROCV `Código` → Transfer Type; se Credit = "Não Cadastrado" → "Não Cadastrado" | Aba Transfer Type |
| **Reason** | `"FARM PYM >" + Vendor Name` | Calculado |
| **AP Type** | Se GL Description contém "inventory" → Produtivo, senão → Consumível | Condicional |
| **GL** | Se Produtivo → GL Description; Se Consumível → "Expense" | Condicional |
| **Status** | Se Paid Date preenchida → PAID, senão → UNPAID | Condicional |
| **Year** | Ano de Paid Date (se PAID), senão "UNPAID" | Extraído |
| **Month** | Mês de Paid Date (se PAID), senão "UNPAID" | Extraído |
| **Week** | Semana ISO de Paid Date (se PAID), senão "UNPAID" | Extraído |
| **Validação** | Se PAID → "PAID"; Se UNPAID → PROCV Vendor#, fallback SUFFIX1 | Aba Validação |
| **Expt Pymt** | Se PAID → "PAID"; Se UNPAID → próxima sexta-feira após Due Date | Calculado |
| **OBS** | Vazio (preenchimento manual) | — |

---

## Detalhamento das Tabelas de Referência

### Currency
Mapeia o código do ERP (`$`, `E`, `G`, etc.) para o nome completo (USD, EUR, GBP...).

### Kyriba (C.A.)
Cadastro de vendors no sistema Kyriba. Usado para buscar:
- **Code** → Credit Account (Kyriba)
- **Country** → Vendor Bank Country
- Chave de busca: **Name 2** (= Vendor #)

### Transfer Type
Define o tipo de transferência bancária por combinação Company + Currency + Country:
- **Código** = Company + Currency + Country (ex: "1USDUS")
- Retorna: Transfer Type (FEDW, SEPD, FAST, DTRF) e Kyriba Debit Account

### Validação
Lista de vendors/GLs com tratamento especial:
- **NOT PAY** — Não deve ser pago (Chase CCC, estornos, autopay)
- **VALIDATE - Lease** — Aluguel com provisionamento
- **VALIDATE - WH AGENT** — Agentes wholesales (prepaid/commission)
- **VALIDATE - PAYROLL** — Folha de pagamento automática
- **VALIDATE - DUTIES** — Adiantamento de duties
- **HOLD** — Pagamento suspenso

---

## Validações Automáticas

O script gera alertas para:

1. **"Não Cadastrado"** — Vendors sem cadastro no Kyriba (Credit Account, Bank Country)
2. **Códigos sem Transfer Type** — Combinações Company/Currency/Country não mapeadas
3. **Original Amount = 0** — Linhas potencialmente incorretas
4. **Validações especiais** — Itens que precisam de análise manual (NOT PAY, VALIDATE, HOLD)
5. **Resumo por Company/Currency** — Visão consolidada dos valores UNPAID

---

## Saída Gerada

O arquivo Excel de saída contém:

| Aba | Conteúdo |
|-----|----------|
| **AP By GL** | Dados completos com todas as 17 colunas calculadas |
| **PYM LIST** | Lista de pagamentos agrupada por vendor (apenas UNPAID) |
| **Resumo** | Tabela dinâmica por AP Type / Company / Currency / GL / Due Date |
| **Transfer Type** | Cópia da tabela de referência |
| **Currency** | Cópia da tabela de referência |
| **Validação** | Cópia da tabela de referência |
| **Kyriba (C.A.)** | Cópia da tabela de referência |

### Formatação
- Cabeçalho azul escuro com fonte branca
- Colunas calculadas (AD+) com fundo azul claro
- Filtro automático ativado
- Painel congelado na linha 1

---

## Próximos Passos Recomendados

1. **Integrar com Redmine**: Incorporar tickets pendentes do Redmine na projeção
2. **Gerar aba Weekly Prod**: Pivotar produtivo por Company/Vendor/GL/semana
3. **Gerar aba Projeção**: Calcular projeção semanal de pagamentos
4. **Interface gráfica**: Criar GUI com tkinter ou PyQt para usuários não-técnicos
5. **Agendamento**: Automatizar execução semanal via Windows Task Scheduler
6. **Validação de saldos Kyriba**: Integrar consulta de saldos bancários para check de liquidez

---

## Troubleshooting

| Problema | Solução |
|----------|---------|
| "Nenhum arquivo encontrado" | Verifique o caminho da pasta `--bases` |
| Muitos "Não Cadastrado" | Atualize a aba Kyriba (C.A.) com novos vendors |
| Transfer Type incorreto | Verifique se há novas combinações Company/Currency/Country na aba Transfer Type |
| Erro de encoding | Salve as bases do ERP em formato .xlsx (não .csv) |
| Dados de Paid Date incorretos | Verifique formato de data no ERP (deve ser datetime) |
