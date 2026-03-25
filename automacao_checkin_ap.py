"""
Automação - Check-in AP: Aba "AP By GL"
========================================
Consolida os 6 relatórios brutos do RLM, aplica todas as transformações
e lookups, e gera o arquivo tratado com as 46 colunas da aba "AP By GL".

USO:
    python automacao_checkin_ap.py

ESTRUTURA DE PASTAS ESPERADA:
    ./relatorios_brutos/     → 6 arquivos .xlsx do RLM
    ./planilhas_suporte/     → currency.xlsx, transfer_type.xlsx, agents.xlsx, validação.xlsx
    ./kyriba_ca.xlsx         → Arquivo exportado do Kyriba (C.A.)
    ./output/                → Arquivo de saída (gerado automaticamente)
"""

import pandas as pd
import numpy as np
import os
import glob
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURAÇÃO DE CAMINHOS
# ============================================================
PASTA_RELATORIOS    = "./relatorios_brutos"
PASTA_SUPORTE       = "./planilhas_suporte"
ARQUIVO_KYRIBA      = "./kyriba_ca.xlsx"
PASTA_SAIDA         = "./output"
NOME_SAIDA          = f"AP_BY_GL_tratado_{datetime.today().strftime('%y%m%d')}.xlsx"


# ============================================================
# CARREGAMENTO DAS PLANILHAS SUPORTE
# ============================================================

def carregar_currency(pasta):
    """Carrega currency.xlsx → dicionário {Vendor Curr: Currency}"""
    path = os.path.join(pasta, "currency.xlsx")
    print(f"    → Currency: {path}")
    df = pd.read_excel(path)
    return dict(zip(df.iloc[:, 0].astype(str).str.strip(), df.iloc[:, 1].astype(str).str.strip()))


def carregar_transfer_type(pasta):
    """Carrega transfer_type.xlsx → dicionários de lookup."""
    path = os.path.join(pasta, "transfer_type.xlsx")
    print(f"    → Transfer Type: {path}")
    df = pd.read_excel(path)

    df['_codigo'] = df.iloc[:, 0].astype(int).astype(str) + df.iloc[:, 1].astype(str).str.strip() + df.iloc[:, 2].astype(str).str.strip()
    df['_codigo2'] = df.iloc[:, 0].astype(int).astype(str) + df.iloc[:, 1].astype(str).str.strip()

    tt_by_codigo = dict(zip(df['_codigo'], df.iloc[:, 5].astype(str).str.strip()))
    tt_debit_by_codigo2 = dict(zip(df['_codigo2'], df.iloc[:, 7].astype(str).str.strip()))
    tt_debit_by_company = dict(zip(df.iloc[:, 0].astype(int), df.iloc[:, 7].astype(str).str.strip()))

    return tt_by_codigo, tt_debit_by_codigo2, tt_debit_by_company


def carregar_validacao(pasta):
    """Carrega validação.xlsx → dicionários por Vendor # e por SUFFIX1."""
    path = os.path.join(pasta, "validação.xlsx")
    if not os.path.exists(path):
        path = os.path.join(pasta, "validacao.xlsx")
    print(f"    → Validação: {path}")
    df = pd.read_excel(path)

    val_by_vendor = {}
    val_by_suffix = {}

    for _, row in df.iterrows():
        suffix = row.iloc[0]
        vendor_num = row.iloc[4]
        validacao = row.iloc[6]

        if pd.notna(validacao):
            validacao = str(validacao).strip()
            if pd.notna(vendor_num) and str(vendor_num).strip() not in ['-', '', 'nan']:
                vendor_key = str(vendor_num).strip().zfill(6)
                val_by_vendor[vendor_key] = validacao
            if pd.notna(suffix) and str(suffix).strip() not in ['-', '', 'nan']:
                try:
                    val_by_suffix[int(float(suffix))] = validacao
                except (ValueError, TypeError):
                    pass

    return val_by_vendor, val_by_suffix


def carregar_agents(pasta):
    """Carrega agents.xlsx."""
    path = os.path.join(pasta, "agents.xlsx")
    print(f"    → Agents: {path}")
    df = pd.read_excel(path)
    df['Vendor#'] = pd.to_numeric(df.iloc[:, -1], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(6)
    return df


def carregar_kyriba(caminho):
    """Carrega Kyriba (C.A.) → dicionários de lookup."""
    print(f"    → Kyriba (C.A.): {caminho}")
    df = pd.read_excel(caminho)
    cols = df.columns.tolist()
    col_vendor  = cols[1]
    col_code    = cols[2]
    col_country = cols[10]

    df[col_vendor] = df[col_vendor].astype(str).str.strip().str.zfill(6)
    kyriba_country = df.set_index(col_vendor)[col_country].to_dict()
    kyriba_credit  = df.set_index(col_vendor)[col_code].to_dict()

    print(f"      {len(kyriba_country)} vendors carregados")
    return kyriba_country, kyriba_credit


# ============================================================
# CARREGAMENTO DOS RELATÓRIOS BRUTOS
# ============================================================

def carregar_relatorios(pasta):
    """Carrega todos os .xlsx da pasta, consolida e remove subtotais."""
    arquivos = glob.glob(os.path.join(pasta, "*.xlsx"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo .xlsx encontrado em: {pasta}")

    print(f"  {len(arquivos)} relatórios encontrados:")
    dfs = []
    for arq in sorted(arquivos):
        print(f"    → {os.path.basename(arq)}")
        df = pd.read_excel(arq)
        dfs.append(df)

    df_all = pd.concat(dfs, ignore_index=True)
    linhas_antes = len(df_all)

    col_company = df_all.columns[0]
    df_all = df_all[df_all[col_company].notna() & (df_all[col_company] != '')]
    df_all = df_all[pd.to_numeric(df_all[col_company], errors='coerce').notna()]

    linhas_depois = len(df_all)
    print(f"  Consolidado: {linhas_antes} → {linhas_depois} linhas ({linhas_antes - linhas_depois} subtotais removidos)")
    return df_all


# ============================================================
# COLUNAS CALCULADAS
# ============================================================

def calcular_colunas(df, kyriba_country, kyriba_credit, currency_map,
                     tt_by_codigo, tt_debit_by_codigo2, tt_debit_by_company,
                     val_by_vendor, val_by_suffix):
    """Aplica todas as 17 colunas calculadas."""
    print("  Aplicando colunas calculadas...")

    # --- Tipos ---
    df['Company #'] = df['Company #'].astype(int)
    df['Vendor #'] = pd.to_numeric(df['Vendor #'], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(6)
    df['Vendor Curr'] = df['Vendor Curr'].astype(str).str.strip()
    df['Exchange Rate'] = pd.to_numeric(df['Exchange Rate'], errors='coerce').fillna(1)
    df['Amount($)'] = pd.to_numeric(df['Amount($)'], errors='coerce').fillna(0)
    df['SUFFIX1 '] = pd.to_numeric(df['SUFFIX1 '], errors='coerce')

    # --- Original Amount ---
    df['Original Amount'] = df['Amount($)'] * df['Exchange Rate']

    # --- Currency ---
    df['Currency'] = df['Vendor Curr'].map(currency_map).fillna(df['Vendor Curr'])

    # --- Vendor Bank Country ---
    df['Vendor Bank Country'] = df['Vendor #'].map(kyriba_country).fillna('Não Cadastrado')

    # --- Credit Account (Kyriba) ---
    df['Credit Account (Kyriba)'] = df['Vendor #'].map(kyriba_credit).fillna('Não Cadastrado')

    # --- Código ---
    df['Código'] = df['Company #'].astype(str) + df['Currency'] + df['Vendor Bank Country']

    # --- Debit Account (Kyriba) ---
    def get_debit_account(row):
        codigo2 = str(row['Company #']) + row['Currency']
        result = tt_debit_by_codigo2.get(codigo2)
        if result:
            return result
        return tt_debit_by_company.get(row['Company #'], '')
    df['Debit Account (Kyriba)'] = df.apply(get_debit_account, axis=1)

    # --- Transfer Type ---
    def get_transfer_type(row):
        if row['Credit Account (Kyriba)'] == 'Não Cadastrado':
            return 'Não Cadastrado'
        result = tt_by_codigo.get(row['Código'])
        return result if result else 'ITRF'
    df['Transfer Type'] = df.apply(get_transfer_type, axis=1)

    # --- Reason ---
    df['Reason'] = 'FARM PYM >' + df['Vendor Name'].astype(str)

    # --- AP Type ---
    df['AP Type'] = df['General Ledger Description'].astype(str).str.lower().apply(
        lambda x: 'Produtivo' if 'inventory' in x else 'Consumível'
    )

    # --- GL ---
    df['GL'] = df.apply(
        lambda row: row['General Ledger Description'] if row['AP Type'] == 'Produtivo' else 'Expense',
        axis=1
    )

    # --- Status ---
    def get_status(paid_date):
        if pd.isna(paid_date):
            return 'UNPAID'
        if isinstance(paid_date, datetime):
            return 'PAID'
        try:
            return 'PAID' if float(paid_date) > 0 else 'UNPAID'
        except (ValueError, TypeError):
            return 'PAID' if paid_date else 'UNPAID'
    df['Status'] = df['Paid Date'].apply(get_status)

    # --- Year / Month / Week ---
    def parse_date_safe(val):
        if pd.isna(val):
            return pd.NaT
        if isinstance(val, datetime):
            return val
        try:
            return pd.to_datetime(val)
        except:
            return pd.NaT

    paid_parsed = df['Paid Date'].apply(parse_date_safe)

    df['Year'] = df.apply(
        lambda r: paid_parsed[r.name].year if r['Status'] == 'PAID' and pd.notna(paid_parsed[r.name]) else 'UNPAID', axis=1)
    df['Month'] = df.apply(
        lambda r: paid_parsed[r.name].month if r['Status'] == 'PAID' and pd.notna(paid_parsed[r.name]) else 'UNPAID', axis=1)
    df['Week'] = df.apply(
        lambda r: paid_parsed[r.name].isocalendar()[1] if r['Status'] == 'PAID' and pd.notna(paid_parsed[r.name]) else 'UNPAID', axis=1)

    # --- Validação ---
    def get_validacao(row):
        if row['Status'] == 'PAID':
            return 'PAID'
        vendor = str(row['Vendor #']).strip()
        result = val_by_vendor.get(vendor)
        if result:
            return result
        suffix = row['SUFFIX1 ']
        if pd.notna(suffix):
            try:
                result = val_by_suffix.get(int(suffix))
                if result:
                    return result
            except (ValueError, TypeError):
                pass
        return '-'
    df['Validação'] = df.apply(get_validacao, axis=1)

    # --- Expt Pymt ---
    def get_expt_pymt(row):
        if row['Status'] == 'PAID':
            return 'PAID'

        validacao = str(row.get('Validação', ''))

        # NOT PAY → "No"
        if validacao.startswith('NOT PAY'):
            return 'No'

        # [GC] no início da Description → "[GC]"
        desc = str(row.get('Description', ''))
        if desc.startswith('[GC]'):
            return '[GC]'

        # Lease ou OZAN/RAFA → replica a Due Date do RLM
        if 'Lease' in validacao or 'OZAN/RAFA' in validacao:
            due = row.get('Due Date')
            if pd.notna(due):
                return due
            return ''

        # Demais UNPAID → próxima sexta-feira
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        days_until_friday = (4 - today.weekday()) % 7
        if days_until_friday == 0:
            days_until_friday = 7
        return today + timedelta(days=days_until_friday)

    df['Expt Pymt'] = df.apply(get_expt_pymt, axis=1)

    # --- OBS ---
    df['OBS'] = None

    print("    → 17 colunas calculadas aplicadas com sucesso")
    return df


# ============================================================
# ORDENAÇÃO E FORMATAÇÃO
# ============================================================

COLUNAS_ORIGINAIS = [
    'Company #', 'Voucher #', 'SUFFIX1 ', 'MAIN ', 'DEPT ',
    'General Ledger Description', 'Season', 'PO #', 'Shipment #',
    'House/Airway Bill #', 'Vendor #', 'Vendor Name', 'Business Type',
    'Entry Date', 'Batch Date', 'Purchase Period', 'Invoice Date',
    'Due Date', 'Paid Date', 'DSO', 'Days Late', 'Check #',
    'Invoice #', 'Description', 'Summ Cost', 'Vendor Curr',
    'Exchange Rate', 'Amount($)', 'Voucher Notes',
]
COLUNAS_CALCULADAS = [
    'Original Amount', 'Currency', 'Vendor Bank Country',
    'Credit Account (Kyriba)', 'Código', 'Debit Account (Kyriba)',
    'Transfer Type', 'Reason', 'GL', 'AP Type', 'Status',
    'Year', 'Month', 'Week', 'Validação',
]
COLUNAS_ANALISE = ['Expt Pymt', 'OBS']


def ordenar_colunas(df):
    ordem = COLUNAS_ORIGINAIS + COLUNAS_CALCULADAS + COLUNAS_ANALISE
    return df[[c for c in ordem if c in df.columns]]


def formatar_excel(caminho_saida, df):
    """Salva o DataFrame com formatação profissional e cores por grupo de colunas."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "AP By GL"

    all_cols = list(df.columns)
    n_orig = len(COLUNAS_ORIGINAIS)
    n_calc = len(COLUNAS_CALCULADAS)

    # Cores de fundo para os headers
    fill_orig    = PatternFill('solid', fgColor='D7E4BC')  # A-AC: verde claro
    fill_calc    = PatternFill('solid', fgColor='C09676')  # AD-AR: marrom
    fill_analise = PatternFill('solid', fgColor='F3EBE4')  # AS-AT: bege

    header_font = Font(name='Calibri', bold=True, size=11)
    header_align = Alignment(horizontal='center', vertical='center', wrap_text=True)
    thin_border = Border(
        left=Side(style='thin', color='D9D9D9'),
        right=Side(style='thin', color='D9D9D9'),
        top=Side(style='thin', color='D9D9D9'),
        bottom=Side(style='thin', color='D9D9D9'),
    )

    # Headers
    for col_idx, col_name in enumerate(all_cols, 1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.font = header_font
        cell.alignment = header_align
        cell.border = thin_border
        if col_idx <= n_orig:
            cell.fill = fill_orig
        elif col_idx <= n_orig + n_calc:
            cell.fill = fill_calc
        else:
            cell.fill = fill_analise

    # Dados
    data_font = Font(name='Calibri', size=9)
    for row_idx, row_data in enumerate(df.values, 2):
        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if isinstance(value, (np.integer, np.int64)):
                cell.value = int(value)
            elif isinstance(value, (np.floating, np.float64)):
                cell.value = float(value)
            elif isinstance(value, np.bool_):
                cell.value = bool(value)
            elif not isinstance(value, str) and pd.isna(value):
                cell.value = None
            else:
                cell.value = value
            cell.font = data_font
            cell.border = thin_border

    # Formatação monetária
    for col_name in ['Amount($)', 'Original Amount']:
        if col_name in all_cols:
            ci = all_cols.index(col_name) + 1
            for r in range(2, len(df) + 2):
                ws.cell(row=r, column=ci).number_format = '#,##0.00'

    # Formatação de datas no Expt Pymt
    if 'Expt Pymt' in all_cols:
        ci = all_cols.index('Expt Pymt') + 1
        for r in range(2, len(df) + 2):
            val = ws.cell(row=r, column=ci).value
            if isinstance(val, datetime):
                ws.cell(row=r, column=ci).number_format = 'MM/DD/YYYY'

    # Auto-ajuste de largura
    for col_idx in range(1, len(all_cols) + 1):
        col_letter = get_column_letter(col_idx)
        max_len = len(str(all_cols[col_idx - 1]))
        for row in range(2, min(len(df) + 2, 102)):
            val = ws.cell(row=row, column=col_idx).value
            if val:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 30)

    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = 'A2'

    wb.save(caminho_saida)
    print(f"    → Arquivo salvo: {caminho_saida}")


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================
def main():
    print("=" * 60)
    print("  AUTOMAÇÃO CHECK-IN AP - Aba AP By GL")
    print("=" * 60)

    print("\n[1/4] Carregando planilhas suporte...")
    currency_map = carregar_currency(PASTA_SUPORTE)
    tt_by_codigo, tt_debit_by_codigo2, tt_debit_by_company = carregar_transfer_type(PASTA_SUPORTE)
    val_by_vendor, val_by_suffix = carregar_validacao(PASTA_SUPORTE)
    agents_df = carregar_agents(PASTA_SUPORTE)
    kyriba_country, kyriba_credit = carregar_kyriba(ARQUIVO_KYRIBA)

    print(f"    Currency: {len(currency_map)} moedas")
    print(f"    Transfer Type: {len(tt_by_codigo)} códigos")
    print(f"    Validação: {len(val_by_vendor)} vendors + {len(val_by_suffix)} SUFFIX")
    print(f"    Agents: {len(agents_df)} agentes")
    print(f"    Kyriba: {len(kyriba_country)} vendors")

    print("\n[2/4] Carregando relatórios brutos...")
    df = carregar_relatorios(PASTA_RELATORIOS)

    print("\n[3/4] Calculando colunas...")
    df = calcular_colunas(df, kyriba_country, kyriba_credit, currency_map,
                          tt_by_codigo, tt_debit_by_codigo2, tt_debit_by_company,
                          val_by_vendor, val_by_suffix)
    df = ordenar_colunas(df)

    print("\n[4/4] Salvando arquivo de saída...")
    os.makedirs(PASTA_SAIDA, exist_ok=True)
    caminho_saida = os.path.join(PASTA_SAIDA, NOME_SAIDA)
    formatar_excel(caminho_saida, df)

    print(f"\n{'=' * 60}")
    print(f"  CONCLUÍDO! {len(df)} linhas processadas")
    print(f"  Saída: {caminho_saida}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
