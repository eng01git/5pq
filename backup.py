# importar bibliotecas
import streamlit as st
import pandas as pd

# Link do arquivo com os dados
DATA_URL = "data.csv"

#Leitura dos dados, cache temporariamente desabilitado por conta da nserção de novos valores
#@st.cache
def load_data():
	data = pd.read_csv(DATA_URL)
	return data

# Carrega dataframe e extrai suas colunas
df = load_data()
colunas = df.columns

#st.write(df.head())

# Definição da sidebar
st.sidebar.title("Escolha a ação desejada")
st.sidebar.checkbox("Inserir ocorrência 5 Porquês")
st.sidebar.checkbox("Avaliar ocorrência 5 Porquês")

# Lista vazia para input dos dados do formulário
lista = []
submitted1=False

# Constantes
equipamentos = ['Uncoiler', 'Cupper']
gatilhos = [ 'Segurança', '10 minutos', '1 hora']
linhas = ['L571', 'L572', 'L581', 'Utilidades']
turnos = ['Turno A', 'Turno B', 'Turno C']
departamentos = ['Engenharia', 'Automação', 'Manutenção']
falhas = ['Máquina', 'Mão-de-obra', 'Método', 'Materiais', 'Meio ambiente', 'Medição', 'Outra']
deterioização = ['Forçada', 'Natural', 'Nenhuma']

# Imagem
st.image('Ambev.jpeg')

# Titulo da aplicação
st.title('Formulário 5 porques')

with st.form('Form1'):
	lista.append(st.date_input('Data da ocorrência'))
	lista.append(st.selectbox('Selecione o turno', turnos ))
	lista.append(st.selectbox('Selecione o departamento', departamentos))
	lista.append(st.selectbox('Selecione a linha', linhas))
	lista.append(st.selectbox('Selecione o equipamento', equipamentos))
	lista.append(st.selectbox('Selecione o gatilho', gatilhos))
	lista.append(st.text_input('Descreva a anomalia', ""))
	lista.append(st.text_input('Ordem de manutenção', ""))
	lista.append(st.text_input('Descreva a correção', ""))
	lista.append(st.text_input('1) Por que?', ""))
	lista.append(st.text_input('2) Por que?', ""))
	lista.append(st.text_input('3) Por que?', ""))
	lista.append(st.text_input('4) Por que?', ""))
	lista.append(st.text_input('5) Por que?', ""))
	lista.append(st.multiselect('Selecione o tipo da falha', falhas))
	lista.append(st.multiselect('Selecione o tipo da deterioização (falha)', deterioização))
	lista.append(st.multiselect('Selecione o tipo da correção', falhas))
	lista.append(st.multiselect('Selecione o tipo da deterioização (correção)', deterioização))
	lista.append(st.text_input('Ações tomadas', ""))
	lista.append(st.text_input('Notas de manutenção', ""))
	lista.append(st.text_input('Responsável pela identificação da anomalia', ""))
	lista.append(st.text_input('Responsável pela correção da anomalia', ""))
	submitted1 = st.form_submit_button('Enviar 5 Porquês')

if submitted1:
	df2 = pd.DataFrame(lista).T
	df2.columns = colunas
	st.write(df2.head())
	#st.write(df2.pivot(columns=colunas))
	
	df = pd.concat([df, df2], join='inner', ignore_index=True)
	df.to_csv(DATA_URL, index=False)

st.write(df.head())
