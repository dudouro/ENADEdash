import streamlit as st
import pandas as pd
import altair as alt
import re # Importar re para ordenação de renda

# --- Configurações da página ---
st.set_page_config(
    page_title="Dashboard ENADE 2022 - Análise Detalhada",
    page_icon="📊",
    layout="wide"
)

# --- Constantes ---
FILE_MAPPING = {
    'tempo': {'fname': 'TEMPO.csv', 'pk': 'TEMPO_KEY'},
    'curso': {'fname': 'CURSO.csv', 'pk': 'CURSO_KEY'},
    'desempenho': {'fname': 'DESEMPENHO.csv'},
    'sexo': {'fname': 'SEXO.csv', 'pk': 'SEXO_KEY'},
    'idade': {'fname': 'IDADE.csv', 'pk': 'IDADE_KEY'},
    'renda': {'fname': 'RENDA.csv', 'pk': 'RENDA_KEY'},
    'cor': {'fname': 'COR.csv', 'pk': 'COR_KEY'},
    'escolaridade': {'fname': 'ESCOLARIDADE.csv', 'pk': 'ESCOLARIDADE_KEY'}
}
FACT_FK_MAPPING = {
    'tempo': 'D_TEMPO_TEMPO_KEY',
    'curso': 'D_CURSO_CURSO_KEY',
    'idade': 'D_IDADE_IDADE_KEY', # Necessário para o gráfico de idade correto
    # Adicione outras FKs se precisar cruzar mais dados demográficos com desempenho
}
TARGET_YEAR = 2022

# --- Funções de Carregamento e Processamento (Mantidas da versão anterior) ---
@st.cache_data
def load_data(file_mapping):
    # ... (código load_data inalterado) ...
    dims = {}
    fact = None
    for key, info in file_mapping.items():
        fname = info['fname']
        try:
            df = pd.read_csv(fname, sep=';', quotechar='"', encoding='utf-8', low_memory=False)
            df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
            for col in df.select_dtypes(include=['object']).columns:
                if df[col].astype(str).str.contains('"').any():
                   df[col] = df[col].astype(str).str.replace('"', '', regex=False).str.strip()
        except FileNotFoundError:
            st.error(f"Erro Crítico: Arquivo não encontrado: {fname}. Verifique o diretório.")
            st.stop()
        except Exception as e:
            st.error(f"Erro ao ler {fname}: {e}")
            st.stop()

        if key == 'desempenho':
            fact = df
        else:
            dims[key] = df
            pk = info.get('pk')
            if pk and pk in df.columns:
                df[pk] = pd.to_numeric(df[pk], errors='ignore')

    if fact is not None:
        for dim_key, fact_fk in FACT_FK_MAPPING.items():
            if fact_fk in fact.columns:
                fact[fact_fk] = pd.to_numeric(fact[fact_fk], errors='ignore')
        if 'NOTA_TOTAL' in fact.columns:
            fact['NOTA_TOTAL'] = pd.to_numeric(fact['NOTA_TOTAL'], errors='coerce')
            fact.dropna(subset=['NOTA_TOTAL'], inplace=True) # Remove linhas onde a nota é NaN
            # Arredonda a nota para 1 casa decimal para reduzir granularidade (opcional, mas recomendado)
            # fact['NOTA_TOTAL'] = fact['NOTA_TOTAL'].round(1)

    return dims, fact


@st.cache_data
def merge_fact(_dims, _fact, dim_key, fact_key):
    # ... (código merge_fact inalterado) ...
    df_dim = _dims.get(dim_key)
    dim_info = FILE_MAPPING.get(dim_key)
    fact_fk_col = FACT_FK_MAPPING.get(fact_key)

    if df_dim is None or _fact is None or dim_info is None or fact_fk_col is None:
        # st.warning(f"Dados insuficientes para fazer merge para a chave '{dim_key}'.") # Menos verboso
        return _fact if _fact is not None else pd.DataFrame()

    dim_pk_col = dim_info.get('pk')
    if not dim_pk_col or dim_pk_col not in df_dim.columns or fact_fk_col not in _fact.columns:
        # st.warning(f"Não foi possível fazer o merge para '{dim_key}': Chave primária '{dim_pk_col}' ou chave estrangeira '{fact_fk_col}' não encontrada.")
        return _fact

    try:
        # Garante tipos compatíveis antes do merge
        if _fact[fact_fk_col].dtype != df_dim[dim_pk_col].dtype:
            try:
                _fact_copy = _fact.copy() # Trabalha com cópia para evitar SettingWithCopyWarning
                _fact_copy[fact_fk_col] = _fact_copy[fact_fk_col].astype(df_dim[dim_pk_col].dtype)
                _fact = _fact_copy
            except Exception as e:
                try:
                     _fact_copy = _fact.copy()
                     df_dim_copy = df_dim.copy()
                     _fact_copy[fact_fk_col] = _fact_copy[fact_fk_col].astype(str)
                     df_dim_copy[dim_pk_col] = df_dim_copy[dim_pk_col].astype(str)
                     _fact = _fact_copy
                     df_dim = df_dim_copy
                     # st.info(f"Convertendo chaves de merge para string para '{dim_key}'.")
                except Exception as e_str:
                     st.error(f"Falha na conversão de tipo para merge '{dim_key}': {e} / {e_str}")
                     return pd.DataFrame()

        merged_df = pd.merge(
            _fact,
            df_dim,
            left_on=fact_fk_col,
            right_on=dim_pk_col,
            how='left'
        )
        return merged_df
    except Exception as e:
        st.error(f"Erro durante o merge entre fato e dimensão {dim_info['fname']}: {e}")
        return pd.DataFrame()


# --- Carregamento dos Dados ---
dims, fact = load_data(FILE_MAPPING)

if fact is None or fact.empty:
    st.error("Tabela Fato (Desempenho) não pôde ser carregada ou está vazia. O Dashboard não pode continuar.")
    st.stop()

# --- Título e informações gerais ---
st.title("📊 Análise Detalhada do ENADE 2022")
st.markdown("""
**Fonte de dados:** Microdados do INEP | **Ano de Análise:** 2022
""")
st.markdown("---")

# --- Pré-processamento e Filtro Inicial por Ano ---
df_merged_tempo = merge_fact(dims, fact, 'tempo', 'tempo')

if 'ANO' in df_merged_tempo.columns:
    df_filtered_year = df_merged_tempo[df_merged_tempo['ANO'] == TARGET_YEAR].copy()
else:
    st.warning("Coluna 'ANO' não encontrada após merge com TEMPO. Exibindo todos os dados disponíveis da tabela fato.")
    df_filtered_year = fact.copy()

if df_filtered_year.empty:
    st.warning(f"Não há dados de desempenho disponíveis para o ano {TARGET_YEAR} após o filtro inicial.")
    st.stop()

# --- Seção 1: Performance Geral ---
st.header("📋 Performance Geral dos Participantes (2022)")

overall_metrics = {} # Dicionário para guardar métricas gerais
with st.container(border=True):
    st.subheader("Estatísticas Descritivas da Nota Total")
    if 'NOTA_TOTAL' in df_filtered_year.columns and not df_filtered_year['NOTA_TOTAL'].empty:
        nota = df_filtered_year['NOTA_TOTAL']
        overall_metrics = {
            "Participantes": int(nota.count()),
            "Média": nota.mean(),
            "Mediana": nota.median(),
            "Mínimo": nota.min(),
            "Máximo": nota.max(),
            "Desvio Padrão": nota.std()
        }

        col1, col2, col3, col4, col5 = st.columns(5)
        # ... (código de exibição das métricas inalterado) ...
        cols = [col1, col2, col3, col4, col5]
        metrics_to_show = {k: v for k, v in overall_metrics.items() if k != 'Participantes'}
        col1.metric("Nº de Participantes", f"{overall_metrics['Participantes']:,}".replace(",", "."))
        metric_items = list(metrics_to_show.items())
        for i, col in enumerate(cols[1:]):
             if i < len(metric_items):
                 label, value = metric_items[i]
                 col.metric(label, f"{value:.2f}")


        # Histograma de distribuição de notas (Usando Altair como antes, é mais flexível)
        st.subheader("Distribuição das Notas")
        hist_geral = alt.Chart(df_filtered_year).mark_bar(color='#4CAF50', opacity=0.7).encode(
            alt.X("NOTA_TOTAL:Q", bin=alt.Bin(maxbins=40), title="Nota Total"),
            alt.Y("count():Q", title="Número de Participantes"),
            tooltip=[
                alt.X("NOTA_TOTAL:Q", bin=alt.Bin(maxbins=40), title="Faixa da Nota"),
                alt.Y("count():Q", title="Número de Participantes", format=',')
            ]
        ).properties(
            # title='Distribuição das Notas Totais - ENADE 2022', # Título já está no subheader
             height=300
        ).interactive()
        st.altair_chart(hist_geral, use_container_width=True)

    else:
        st.warning("Não foi possível calcular as estatísticas de desempenho (Coluna 'NOTA_TOTAL' ausente ou vazia).")

st.markdown("---")

# --- Seção 2: Análise Demográfica ---
st.header("👥 Perfil Demográfico dos Participantes")
st.markdown("Distribuição dos participantes por características demográficas.")

col_demo1, col_demo2 = st.columns(2)

# --- Sexo (Com porcentagens claras) ---
with col_demo1:
    with st.container(border=True):
        st.subheader("Distribuição por Sexo")
        # Sexo - Pizza com porcentagens
        sx = dims['sexo'][['QTD_MASCULINO','QTD_FEMININO','QTD_N_INFORMADO']].sum()
        sx.index = ['Masculino','Feminino','Não Informado']
        pie_sx = pd.DataFrame({'Categoria': sx.index, 'Quantidade': sx.values})
        pie_sx = pie_sx[pie_sx['Quantidade'] > 0]
        total_sx = pie_sx['Quantidade'].sum()
        pie_sx['Percent'] = pie_sx['Quantidade'] / total_sx
        # Cores específicas por sexo
        color_scale_sex = alt.Scale(domain=['Masculino','Feminino','Não Informado'], range=['#7B68EE','#EE82EE','#d3d3d3'])
        # Construção do gráfico
        t_base = alt.Chart(pie_sx).encode(theta=alt.Theta('Percent:Q', stack=True))
        pie_sex = t_base.mark_arc(innerRadius=50, outerRadius=100).encode(
            color=alt.Color('Categoria:N', scale=color_scale_sex, legend=alt.Legend(title='Sexo')),
            tooltip=[alt.Tooltip('Categoria:N', title='Sexo'), alt.Tooltip('Quantidade:Q', title='Quantidade'), alt.Tooltip('Percent:Q', title='Percentual', format='.1%')]
        )
        text_sex = t_base.mark_text(radius=120, size=12).encode(
            text=alt.Text('Percent:Q', format='.1%'),
            color=alt.value('black')
        )
        st.altair_chart(pie_sex + text_sex, use_container_width=True)

# --- Cor/Raça (Com porcentagens claras) ---
with col_demo2:
    with st.container(border=True):
        st.subheader("Distribuição por Cor/Raça")
        # Cor/Raça - Pizza com porcentagens e cores específicas
        cr = dims['cor'].filter(like='QTD_').sum()
        cr.index = (cr.index.str.replace('QTD_','', regex=False)
                    .str.replace('_',' ', regex=False)
                    .str.title()
                    .str.replace('Nao ','Não ', regex=False))
        pie_cr = pd.DataFrame({'Categoria':cr.index,'Quantidade':cr.values})
        # Filtrar valores > 0
        pie_cr = pie_cr[pie_cr['Quantidade'] > 0].copy()
        # Calcular porcentagem
        total = pie_cr['Quantidade'].sum()
        pie_cr['Percent'] = pie_cr['Quantidade'] / total
        # Definir escala de cores por categoria
        color_scale = alt.Scale(domain=[
            'Branca','Preta','Parda','Amarela','Indigena','Não Declarada'
        ], range=[
            '#FFDEAD', '#8B4513', '#CD853F', '#F4A460', '#DAA520', '#D3D3D3'
        ])
        # Gráfico de pizza
        base = alt.Chart(pie_cr).encode(theta=alt.Theta('Percent:Q', stack=True))
        slice = base.mark_arc(innerRadius=50, outerRadius=100).encode(
            color=alt.Color('Categoria:N', scale=color_scale, legend=alt.Legend(title='Cor/Raça')),  
            tooltip=[alt.Tooltip('Categoria:N', title='Cor/Raça'), alt.Tooltip('Quantidade:Q'), alt.Tooltip('Percent:Q', format='.1%')]
        )
        # Labels de porcentagem
        text = base.mark_text(radius=120, size=12).encode(
            text=alt.Text('Percent:Q', format='.1%'),
            color=alt.value('black')
        )
        st.altair_chart(slice + text, use_container_width=True)

# --- Idade (Inalterado - já é barra) ---
st.subheader("Distribuição de Idade dos Participantes")
with st.container(border=True):
    # Gráfico de barras para faixa etária
    if 'IDADE' in dims['idade'].columns:
        idade_counts = dims['idade']['IDADE'].value_counts().reset_index()
        idade_counts.columns = ['Faixa Etária', 'Quantidade']
        bar_idade = alt.Chart(idade_counts).mark_bar(color='#17becf').encode(
            x=alt.X('Faixa Etária:N', sort='-y'),
            y='Quantidade:Q',
            tooltip=['Faixa Etária', 'Quantidade']
        ).properties(height=250)
        st.altair_chart(bar_idade, use_container_width=True)


st.markdown("---")

# --- Seção 3: Análise Socioeconômica ---
st.header("💰 Contexto Socioeconômico")
col_socio1, col_socio2 = st.columns(2)

# --- Renda Familiar (Inalterado - já é barra) ---
with col_socio1:
    with st.container(border=True):
        st.subheader("Renda Familiar Mensal")
        # ... (código do gráfico de renda inalterado) ...
        renda_df = dims.get('renda')
        if renda_df is not None and not renda_df.empty:
            r_cols = [c for c in renda_df.columns if c.startswith('QTD_RENDA')]
            if r_cols:
                r_counts = renda_df[r_cols].sum()
                r_counts.index = r_counts.index.str.replace('QTD_RENDA_', '', regex=False)\
                                               .str.replace('_', ' a ', regex=False)\
                                               .str.replace('ATE ', 'Até ', regex=False)\
                                               .str.replace('ACIMA DE ', 'Acima de ', regex=False)\
                                               .str.replace(' SM', ' SM', regex=False)
                renda_data = pd.DataFrame({'Faixa de Renda': r_counts.index, 'Quantidade': r_counts.values})
                renda_data = renda_data[renda_data['Quantidade'] > 0]
                def get_sort_key(renda_str):
                    if 'Não Sabe' in renda_str or 'Não Informado' in renda_str: return float('inf')
                    match = re.search(r'(\d+[\.,]?\d*)', renda_str)
                    if match:
                        num_str = match.group(1).replace(',', '.')
                        return float(num_str)
                    return float('inf') - 1
                unique_renda_categories = sorted(renda_data['Faixa de Renda'].unique(), key=get_sort_key)
                if not renda_data.empty:
                    renda_chart = alt.Chart(renda_data).mark_bar(color='#9467bd', opacity=0.8).encode(
                        x=alt.X('Faixa de Renda', sort=unique_renda_categories, title='Faixa de Renda (Salários Mínimos)'),
                        y=alt.Y('Quantidade:Q', title='Número de Estudantes'),
                        tooltip=['Faixa de Renda', alt.Tooltip('Quantidade:Q', format=',')]
                    ).properties(height=300).interactive()
                    st.altair_chart(renda_chart, use_container_width=True)
                else: st.info("Sem dados de renda para exibir.")
            else: st.warning("Nenhuma coluna ('QTD_RENDA*') encontrada nos dados de renda.")
        else: st.warning("Dados de renda não disponíveis.")


# --- Escolaridade dos Pais (GRÁFICO ESPELHADO / BORBOLETA) ---
with col_socio2:
    with st.container(border=True):
        # Escolaridade (Borboleta)
        st.subheader("Escolaridade dos Pais x Mães")
        esc = dims['escolaridade'].filter(regex='QTD_(PAI|MAE)_').sum()
        # DataFrame longo
        long = esc.reset_index()
        long.columns = ['Categoria','Quantidade']
        # Extrai parentesco e nível
        long[['Parentesco','Nível']] = long['Categoria'].str.extract(r'QTD_(PAI|MAE)_(.+)')
        long['Nível'] = long['Nível'].str.replace('_',' ').str.title()
        # Filtra quantidade >0
        long = long[long['Quantidade']>0]
        # Preparar QtdPlot
        long['QtdPlot'] = long.apply(lambda r: -r['Quantidade'] if r['Parentesco']=='PAI' else r['Quantidade'], axis=1)
        # Ordem dos níveis
        order = long.groupby('Nível')['Quantidade'].sum().abs().sort_values().index.tolist()
        # Máximo para domínio simétrico
        maxv = long['Quantidade'].max()
        # Borboleta
        butter = alt.Chart(long).mark_bar().encode(
            x=alt.X('QtdPlot:Q', title='Quantidade',
                    scale=alt.Scale(domain=[-maxv,maxv]),
                    axis=alt.Axis(labelExpr="datum.value<0?-datum.value:datum.value")),
            y=alt.Y('Nível:N', sort=order, title='Nível de Escolaridade'),
            color=alt.Color('Parentesco:N', legend=alt.Legend(title='Parentesco')),
            tooltip=['Parentesco','Quantidade','Nível']
        ).properties(height=300)
        st.altair_chart(butter, use_container_width=True)

st.markdown("---")

# --- Seção 4: Desempenho por Curso ---
st.header("🎓 Desempenho por Curso")

# Merge com CURSO
df_course_merged = merge_fact(dims, df_filtered_year, 'curso', 'curso')

if not df_course_merged.empty and 'DESC_CURSO' in df_course_merged.columns and 'NOTA_TOTAL' in df_course_merged.columns:

    # 1. Calcular estatísticas (incluindo contagem) para usar no filtro e na cor
    course_stats = df_course_merged.groupby('DESC_CURSO')['NOTA_TOTAL'].agg(['mean', 'count', 'median']).reset_index()
    course_stats.rename(columns={'mean': 'Nota Média', 'count': 'Num Estudantes', 'median': 'Nota Mediana'}, inplace=True)
    course_stats_sorted = course_stats.sort_values('Nota Média', ascending=False)


    # 2. Slider para filtrar por número mínimo de participantes
    min_students_slider = st.slider(
        "Filtrar cursos com mínimo de participantes:",
        min_value=int(course_stats['Num Estudantes'].min()),
        max_value=int(course_stats['Num Estudantes'].quantile(0.95)), # Limita max do slider
        value=max(10, int(course_stats['Num Estudantes'].quantile(0.1))), # Valor inicial
        step=10
    )

    # 3. Obter a lista de cursos que atendem ao critério do slider
    courses_to_show = course_stats[course_stats['Num Estudantes'] >= min_students_slider]['DESC_CURSO'].tolist()

    st.subheader(f"Distribuição das Notas por Curso (≥ {min_students_slider} participantes)")
    st.caption("Boxplots mostram a distribuição das notas (mediana, quartis, min/máx). A cor da caixa indica o número de participantes.")

    if courses_to_show:
        # 4. Filtrar os dados *originais* (com notas individuais) para os cursos selecionados
        filtered_df_for_boxplot = df_course_merged[df_course_merged['DESC_CURSO'].isin(courses_to_show)].copy()

        # 5. Adicionar a coluna 'Num Estudantes' a este dataframe filtrado para usar na cor
        num_students_map = course_stats.set_index('DESC_CURSO')['Num Estudantes']
        filtered_df_for_boxplot['Num Estudantes'] = filtered_df_for_boxplot['DESC_CURSO'].map(num_students_map)

        # --- GRÁFICO DE BOXPLOT VERTICAL ---
        boxplot_chart = alt.Chart(filtered_df_for_boxplot).mark_boxplot(
            extent='min-max',
            outliers=True,
            size=20,
            ticks=True
        ).encode(
            # Eixo X: Curso, ordenado pela mediana da Nota Total
            x=alt.X('DESC_CURSO:N',
                    title='Curso',
                    # CORREÇÃO: Usar EncodingSortField que aceita 'op'
                    sort=alt.EncodingSortField(field="NOTA_TOTAL", op="median", order='descending'),
                    axis=alt.Axis(labelAngle=-60)),

            # Eixo Y: Notas Totais (usadas para calcular o boxplot)
            y=alt.Y('NOTA_TOTAL:Q',
                    title='Distribuição da Nota Total',
                    scale=alt.Scale(zero=False)),

            # Cor: Mapeada para o Número de Estudantes (Quantitativo)
            color=alt.Color('Num Estudantes:Q',
                            title='Nº Participantes',
                            scale=alt.Scale(scheme='viridis'),
                            legend=alt.Legend(orient="top", titleOrient="left")),

            # Tooltip: Mostra estatísticas do boxplot calculadas pelo Altair
            tooltip=[
                alt.Tooltip('DESC_CURSO', title='Curso'),
                alt.Tooltip('Num Estudantes:Q', title='Nº Participantes', format=','),
                alt.Tooltip('median(NOTA_TOTAL):Q', title='Mediana', format='.2f'),
                alt.Tooltip('q1(NOTA_TOTAL):Q', title='1º Quartil (Q1)', format='.2f'),
                alt.Tooltip('q3(NOTA_TOTAL):Q', title='3º Quartil (Q3)', format='.2f'),
                alt.Tooltip('min(NOTA_TOTAL):Q', title='Mínimo (whiskers)', format='.2f'),
                alt.Tooltip('max(NOTA_TOTAL):Q', title='Máximo (whiskers)', format='.2f')
            ]
        ).properties(
            height=500
        ).interactive() # Permite zoom e pan

        st.altair_chart(boxplot_chart, use_container_width=True)
        # --- FIM DO GRÁFICO DE BOXPLOT ---

    else:
        st.info(f"Nenhum curso encontrado com {min_students_slider} ou mais participantes.")

    st.markdown("---")

    # --- Comparativo Detalhado (mantido igual, usa course_stats_sorted) ---
    st.subheader("🔍 Comparativo Detalhado por Curso")
    # Usa a lista de cursos ordenada pela média para o selectbox
    cursos_disponiveis_select = course_stats_sorted['DESC_CURSO'].tolist()
    selected_course = st.selectbox("Selecione um curso para análise detalhada:", options=cursos_disponiveis_select)
    # ... (resto do código para métricas detalhadas permanece o mesmo) ...
    if selected_course:
        course_data_selected = df_course_merged[df_course_merged['DESC_CURSO'] == selected_course]
        if not course_data_selected.empty and 'NOTA_TOTAL' in course_data_selected.columns:
             with st.container(border=True):
                st.markdown(f"**Estatísticas do Curso: {selected_course}**")
                metrics_course = {
                    "Participantes": int(course_data_selected['NOTA_TOTAL'].count()),
                    "Média": course_data_selected['NOTA_TOTAL'].mean(),
                    "Mediana": course_data_selected['NOTA_TOTAL'].median(),
                    "Mínimo": course_data_selected['NOTA_TOTAL'].min(),
                    "Máximo": course_data_selected['NOTA_TOTAL'].max(),
                    "Desvio Padrão": course_data_selected['NOTA_TOTAL'].std()
                }
                col_c1, col_c2, col_c3, col_c4, col_c5 = st.columns(5)
                cols_c = [col_c1, col_c2, col_c3, col_c4, col_c5]
                col_c1.metric("Nº de Participantes", f"{metrics_course['Participantes']:,}".replace(",", "."))
                metrics_course_to_show = {k: v for k, v in metrics_course.items() if k != 'Participantes'}
                mc_items = list(metrics_course_to_show.items())
                for i, col in enumerate(cols_c[1:]):
                    if i < len(mc_items):
                        label, value = mc_items[i]
                        geral_value = overall_metrics.get(label)
                        delta_value_str = None
                        if geral_value is not None and pd.notna(geral_value) and pd.notna(value):
                             delta_value = value - geral_value
                             delta_value_str = f"{delta_value:+.2f}"
                        col.metric(label=label, value=f"{value:.2f}", delta=delta_value_str)
        else:
            st.warning(f"Não há dados de notas válidos para o curso selecionado: {selected_course}")


else:
    st.info("Seção de desempenho por curso não pode ser exibida devido à falta de dados ou colunas necessárias ('DESC_CURSO', 'NOTA_TOTAL') após o merge com a dimensão Curso.")


# --- Rodapé ---
st.markdown("---")
st.caption(f"Dashboard ENADE {TARGET_YEAR} | Análise de Desempenho e Perfil dos Participantes.")
