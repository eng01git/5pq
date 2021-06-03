######################################################################################################
                                           #Introdução
######################################################################################################
# O sistema desenvolvido coleta os dados dos 5 porques através de um formulário web e armazena num  
# banco no-SQL. Esses dados são ligos e disponibilizados para visualização e edição

# Tecnologias:
# Streamlit para web, streamlit share para deploy, banco de dados Firebase (Google)

# Link:
# https://share.streamlit.io/eng01git/5pq/main/5pq.py
######################################################################################################
                                 # importar bibliotecas
######################################################################################################

import streamlit as st
from streamlit_tags import st_tags
from streamlit import caching
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
import json
import smtplib
import time
import base64
from io import BytesIO

from google.cloud import firestore
from google.oauth2 import service_account

######################################################################################################
				#Configurações da página
######################################################################################################

st.set_page_config(
     page_title="Ambev 5-Porques",
     layout="wide",
)

######################################################################################################
				#Configurando acesso ao firebase
######################################################################################################

key_dict = json.loads(st.secrets["textkey"])
creds = service_account.Credentials.from_service_account_info(key_dict)
db = firestore.Client(credentials=creds, project="st-5why")
doc_ref = db.collection(u'5porques')

# Link do arquivo com os dados
DATA_URL = "data.csv"

######################################################################################################
				     #Definição da sidebar
######################################################################################################

st.sidebar.title("Menu 5-Porques")
func_escolhida = st.sidebar.radio('Selecione a opção desejada',('Pendências', 'Inserir', 'Consultar', 'Estatísticas'), index=0)

######################################################################################################
                               #Função para leitura do banco (Firebase)
######################################################################################################
# Efetua a leitura de todos os documentos presentes no banco e passa para um dataframe pandas
# Função para carregar os dados do firebase (utiliza cache para agilizar a aplicação)
@st.cache
def load_data():
	data = pd.read_csv(DATA_URL)
	posts_ref = db.collection("5porques_2")	
	for doc in posts_ref.stream():
		dicionario = doc.to_dict()
		dicionario['document'] = doc.id
		data = data.append(dicionario, ignore_index=True)

	data['data'] = pd.to_datetime(data['data']).dt.date
	data['hora'] = pd.to_datetime(data['hora']).dt.time
	return data

@st.cache
def load_mes():
	dicionario = {}
	posts_ref = db.collection("MES_data")	
	for doc in posts_ref.stream():
		dic_auxiliar = doc.to_dict()
		dicionario[dic_auxiliar['documento']] = dic_auxiliar
			   
	mes_df = pd.DataFrame.from_dict(dicionario)
	mes_df = mes_df.T
	mes_df.reset_index(inplace=True)
	mes_df.drop('index', axis=1, inplace=True)
	lista_colunas = ['Linha', 'Data', 'Hora', 'Tempo', 'Micro/Macro', 'Definição do Evento', 'Nome', 'Equipamento','Ponto Produtivo', 'SubConjunto', 'Componente', 'Modo de Falha - Sintoma', 'Descrição', 'Lote', 'Resultante', 'FluxoProduto', 'FluxoIntervalo', 'Turno', 'Gargalo', 'FiltroExterna', 'documento']
	mes_df = mes_df.reindex(columns=lista_colunas)
	mes_df['Data'] = pd.to_datetime(mes_df['Data']).dt.date
	mes_df['Hora'] = pd.to_datetime(mes_df['Hora']).dt.time
	mes_df['Turno'] = mes_df['Turno'].map({'Morning': 'Turno A', 'Afternoon': 'Turno B', 'Evening': 'Turno C'})
	return mes_df

def upload_mes(uploaded_file, tipos):
	#Leitura dos dados do arquivo excel
	data = pd.read_excel(uploaded_file, sheet_name='Parada')
	#filtrando os dados (tempo maior que 30 e eventos incluídos em tipo)
	data = data[(data['Tempo'] > 30.0)]
	data = data[data['Definição do Evento'].isin(tipos)]
	#ajuste da variável de data
	data['Data'] = data['Data'].dt.date
	#criação do nome do documento
	data['documento'] = data['Linha'].astype(str) + data['Equipamento'].astype(str) + data['Data'].astype(str) + data['Hora'].astype(str)
	
	#cria dicionário vazio
	dicionario = {}
	#importa valores do firebase
	posts_ref = db.collection("MES_data")	
	for doc in posts_ref.stream():
		dic_auxiliar = doc.to_dict()
		dicionario[dic_auxiliar['documento']] = dic_auxiliar
		
	#Filtra os valores presentes no arquivo e não presentes na base dados
	to_include = data[~data['documento'].isin(dicionario.keys())]
	
	#Se houver variáveis a serem incluídas e faz a inclusão
	if to_include.shape[0] > 0 :
		batch = db.batch()
		for index, row in to_include.iterrows():
			ref = db.collection('MES_data').document(row['documento'])
			row_string = row.astype(str)
			batch.set(ref, row_string.to_dict())
		batch.commit()		      
	
	return to_include


# Efetua a leitura dos dados dos usuários no banco
@st.cache
def load_usuarios():
	data = pd.DataFrame(columns=['Nome', 'Email', 'Gestor', 'Codigo'])
	posts_ref = db.collection("Usuarios")	
	for doc in posts_ref.stream():
		dicionario = doc.to_dict()
		dicionario['document'] = doc.id
		data = data.append(dicionario, ignore_index=True)
	return data

# Efetua a leitura das pendencias no banco
@st.cache
def load_pendencias():
	data = pd.DataFrame(columns=['data', 'turno', 'linha', 'equipamento', 'departamento', 'usuario', 'descrição'])
	posts_ref = db.collection("pendencias")	
	for doc in posts_ref.stream():
		dicionario = doc.to_dict()
		dicionario['document'] = doc.id
		data = data.append(dicionario, ignore_index=True)
	return data

# Efetua a leitura dos dados das linhas e dos equipamentos
@st.cache
def load_sap_nv3():
	data = pd.read_csv('SAP_nivel3.csv', sep=';')
	return data

######################################################################################################
                                           #Função para enviar email
######################################################################################################
# Recebe como parâmetros destinatário e um código de atividade para o envio
# O email está configurado por parâmetros presentes no streamlit share (secrets)
def send_email(to, atividade, documento, comentario, gatilho):
	gmail_user = st.secrets["email"]
	gmail_password = st.secrets["senha"]
	sent_from = gmail_user
	from_ = 'Ambev 5 Porques'
	subject = ""
	body = ''
	atividade = int(atividade)
	
	if atividade == 0:
		body = "Ola, foi gerada um novo 5-Porques, acesse a plataforma para avaliar.\nhttps://share.streamlit.io/eng01git/5pq/main/5pq.py\n\nAtenciosamente, \nAmbev 5-Porques"
		subject = """Gerado 5-Porques %s""" % (documento)
	elif atividade == 1:
		body = "Ola, o responsavel retificou 5-Porques, acesse a plataforma para reavaliar.\nhttps://share.streamlit.io/eng01git/5pq/main/5pq.py\n\nAtenciosamente, \nAmbev 5-Porques"
		subject = """Retificado 5-Porques %s""" % (documento)
	elif atividade == 2:
		body = """Ola, o gestor aprovou 5-Porques.\n\n%s \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario)
		subject = """Aprovado 5-Porques %s""" % (documento)	
	elif atividade == 3:
		body = """Ola, o gestor reprovou 5-Porques, acesse a plataforma para retificar.\nhttps://share.streamlit.io/eng01git/5pq/main/5pq.py \n\n Comentario do gestor: \n\n%s  \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario)
		subject = """Reprovado 5-Porques %s""" % (documento)
		
	list_to = [to]
	if int(gatilho) > 60:	
		list_to.append('marius.lisboa@gmail.com')
		list_to.append('BRMAI0514@ambev.com.br')
	
	email_text = """From: %s\nTo: %s\nSubject: %s\n\n%s
	""" % (from_, list_to, subject, body)
	
	try:
		server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
		server.ehlo()
		server.login(gmail_user, gmail_password)
		server.sendmail(sent_from, list_to, email_text.encode('latin-1'))
		server.close()
		st.write('E-mail enviado!')
	except:
		st.error(list_to)

######################################################################################################
                                           #Função para download
######################################################################################################
#download csv		
def download(df):
	"""Generates a link allowing the data in a given panda dataframe to be downloaded
	in:  dataframe
	out: href string
	"""
	csv = df.to_csv(index=False)
	b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
	href = f'<a href="data:file/csv;base64,{b64}">Download dos dados como csv</a>'
	return href
#excel
def to_excel(df):
	output = BytesIO()
	writer = pd.ExcelWriter(output, engine='xlsxwriter')
	df.to_excel(writer, sheet_name='Sheet1')
	writer.save()
	processed_data = output.getvalue()
	return processed_data

def get_table_download_link(df):
	"""Generates a link allowing the data in a given panda dataframe to be downloaded
	in:  dataframe
	out: href string
	"""
	val = to_excel(df)
	b64 = base64.b64encode(val)  # val looks like b'...'
	return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="dados.xlsx">Download dos dados em Excel</a>' # decode b'abc' => abc

			   
######################################################################################################
                                           #Avaliação e edição das ocorrências
######################################################################################################
# Função para aprovar ou reprovar a ocorrência. Permite também a edição de ocorrências passadas,
# possibilitando a retificação das mesmas. Edição através de formulário que aparece preenchido com
# os valores passados anteriormente

def func_validar(index, row, indice):
	
	if row['document'] in indice:
		editar = st.checkbox('Editar 5-Porques ' + str(row['document']))
		
		if not editar:
			st.table(row)
			st.subheader('Avaliação do 5-Porques')
			codigo_gestor = st.text_input('Inserir código do gestor', type='password')
			comentario = st.text_input('Envie um comentário sobre 5-Porques' + ' (' + str(index) + '):',"")			
			bt1, bt2 = st.beta_columns(2)
			aprovar = bt1.button('Aprovar 5-Porques ' + '(' + str(index) + ')')
			reprovar = bt2.button('Reprovar 5-Porques ' + '(' + str(index) + ')')
			st.subheader('Exportar 5-Porques')	
			
			#Download do documento selecionado
			export = filtrado[filtrado['document'] == row['document']]
			st.markdown(get_table_download_link(export), unsafe_allow_html=True)
					
			if aprovar:
				if codigo_gestor == 'GestorAmbev':
					caching.clear_cache()
					att_verificado = {}
					att_verificado['status'] = 'Aprovado'
					db.collection("5porques_2").document(row['document']).update(att_verificado)
					send_email(row['email responsável'], 2, str(row['document']), comentario, 0)
				else:
					st.error('Código do gestor incorreto')

			if reprovar:
				if codigo_gestor != 'GestorAmbev':
					st.error('Código do gestor incorreto')
				elif comentario != '':
					caching.clear_cache()
					att_verificado = {}
					att_verificado['status'] = 'Reprovado'
					db.collection("5porques_2").document(row['document']).update(att_verificado)
					send_email(row['email responsável'], 3, str(row['document']), comentario, 0)
				else: 
					st.error('Obrigatório o preenchimento do comentário!')

		else:
			documento = str(row['document'])	
			doc = row.to_dict()
			sp2, sp3, st0 = st.beta_columns(3)
			list_linhas = list(linhas)
			sap_nv2 = sp2.selectbox('Selecione a linha' + ' (' + str(index) + '):', list_linhas, list_linhas.index(doc['linha']))
			equipamentos = list(sap_nv3[sap_nv3['Linha'] == sap_nv2]['equipamento'])
			
			if sap_nv2 != doc['linha']:
				equipamento_ant = 0
			else:
				equipamento_ant = equipamentos.index(doc['equipamento'])	
			
			with st.form('Form_edit' + str(index)):
				st1, st2, st3, st4 = st.beta_columns(4)
				dic['data'] = st1.date_input('Data da ocorrência' + ' (' + str(index) + '):', doc['data'])
				dic['turno'] = st2.selectbox('Selecione o turno' + ' (' + str(index) + '):', turnos, turnos.index(doc['turno']))
				dic['hora'] = st3.time_input('Selecione o horário' + ' (' + str(index) + '):', value=doc['hora'])
				dic['definição do evento'] = st4.selectbox('Definição do Evento' + ' (' + str(index) + '):', tipos, tipos.index(doc['definição do evento']))
				dic['linha'] = sap_nv2
				dic['equipamento'] = sp3.selectbox('Selecione o equipamento' + ' (' + str(index) + '):', equipamentos, equipamento_ant)
				dic['gatilho'] = st0.number_input('Gatilho em minutos (mínimo 30 min)' + ' (' + str(index) + '):', value=int(doc['gatilho']), min_value=30)
				dic['descrição anomalia'] = st.text_input('Descreva a anomalia' + ' (' + str(index) + '):', value=doc['descrição anomalia'])
				st4, st5 = st.beta_columns(2)
				dic['correção'] = st.text_input('Descreva a correção' + ' (' + str(index) + '):', value=doc['correção'])
				st6, st7 = st.beta_columns(2)
				dic['pq1'] = st.text_input('1) Por que?' + ' (' + str(index) + '):', value=doc['pq1'])
				dic['pq2'] = st.text_input('2) Por que?' + ' (' + str(index) + '):', value=doc['pq2'])
				dic['pq3'] = st.text_input('3) Por que?' + ' (' + str(index) + '):', value=doc['pq3'])
				dic['pq4'] = st.text_input('4) Por que?' + ' (' + str(index) + '):', value=doc['pq4'])
				dic['pq5'] = st.text_input('5) Por que?' + ' (' + str(index) + '):', value=doc['pq5'])
				dic['tipo de falha'] = st4.multiselect('Selecione o tipo da falha' + ' (' + str(index) + '):', falhas)
				dic['falha deterioização'] = st5.multiselect('Selecione o tipo da deterioização (falha)' + ' (' + str(index) + '):', deterioização)
				dic['tipo de correção'] = st6.multiselect('Selecione o tipo da correção' + ' (' + str(index) + '):', falhas)
				dic['correção deterioização'] = st7.multiselect('Selecione o tipo da deterioização (correção)' + ' (' + str(index) + '):', deterioização)
				dic['ações'] = st.text_input('Ações' + ' (' + str(index) + '):', value=doc['ações'])
				st8, st9 = st.beta_columns(2)
				dic['responsável identificação'] = st8.text_input('Responsável pela identificação' + ' (' + str(index) + '):', value=doc['responsável identificação'])
				dic['responsável reparo'] = st9.text_input('Responsável pela correção' + ' (' + str(index) + '):',value=doc['responsável reparo'])
				dic['email responsável'] = st.text_input('E-mail do responsável pelo formulário' + ' (' + str(index) + '):', value=doc['email responsável'])
				dic['gestor'] = st.selectbox('Coordenador' + ' (' + str(index) + '):', gestores, gestores.index(doc['gestor']))
				dic['notas de manutenção'] = st_tags(label=('Notas de manutenção' + ' (' + str(index) + '):'), text='Pressione enter', value=doc['notas de manutenção'].replace(']', '').replace('[','').replace("'",'').split(','))
				dic['ordem manutenção'] = st_tags(label=('Ordem de manutenção' + ' (' + str(index) + '):'), text='Pressione enter', value=doc['ordem manutenção'].replace(']', '').replace('[','').replace("'",'').split(','))
				dic['status'] = 'Retificado'
				submitted_edit = st.form_submit_button('Editar 5 Porquês' + ' (' + str(index) + '):')

			if submitted_edit:
				keys_values = dic.items()
				new_d = {str(key): str(value) for key, value in keys_values}
				for key, value in new_d.items():
					if (value == '') or value == '[]':
						new_d[key] = 'Não informado'
				if '@ambev.com.br' in new_d['email responsável']:
					db.collection("5porques_2").document(documento).set(new_d,merge=True)
					editar = False
					email_gestor = usuarios_fb[usuarios_fb['Nome'] == new_d['gestor']]['Email']
					send_email(str(email_gestor.iloc[0]), 1, documento, '', new_d['gatilho'])
					caching.clear_cache()
				else:
					st.error('Por favor inserir e-mail Ambev válido')
					
######################################################################################################
                                           #Formulário para inclusão de ocorrência
######################################################################################################

def formulario(linhas):
	sp2, sp3, st0 = st.beta_columns(3)
	list_linhas = list(linhas)
	sap_nv2 = sp2.selectbox('Selecione a linha', list_linhas)	
	equipamentos = list(sap_nv3[sap_nv3['Linha'] == sap_nv2]['equipamento'])
	
	with st.form('Form_ins'):
		st1, st2, st3, st4 = st.beta_columns(4)
		dic['data'] = st1.date_input('Data da ocorrência')
		dic['turno'] = st2.selectbox('Selecione o turno', turnos )
		dic['hora'] = st3.time_input('Selecione o horário')
		dic['definição do evento'] = st4.selectbox('Definição do Evento', tipos)
		dic['linha'] = sap_nv2
		dic['equipamento'] = sp3.selectbox('Selecione o equipamento', equipamentos)
		dic['gatilho'] = st0.number_input('Gatilho em minutos (mínimo 30 min)', min_value=30)		
		dic['descrição anomalia'] = st.text_input('Descreva a anomalia', "")
		st4, st5 = st.beta_columns(2)
		dic['correção'] = st.text_input('Descreva a correção', "")
		st6, st7 = st.beta_columns(2)
		dic['pq1'] = st.text_input('1) Por que?', "")
		dic['pq2'] = st.text_input('2) Por que?', "")
		dic['pq3'] = st.text_input('3) Por que?', "")
		dic['pq4'] = st.text_input('4) Por que?', "")
		dic['pq5'] = st.text_input('5) Por que?', "")
		dic['tipo de falha'] = st4.multiselect('Selecione o tipo da falha', falhas)
		dic['falha deterioização'] = st5.multiselect('Selecione o tipo da deterioização (falha)', deterioização)
		dic['tipo de correção'] = st6.multiselect('Selecione o tipo da correção', falhas)
		dic['correção deterioização'] = st7.multiselect('Selecione o tipo da deterioização (correção)', deterioização)
		dic['ações'] = st.text_input('Ações', "")
		st8, st9 = st.beta_columns(2)
		dic['responsável identificação'] = st8.text_input('Responsável pela identificação')
		dic['responsável reparo'] = st9.text_input('Responsável pela correção')
		dic['email responsável'] = st.text_input('E-mail do responsável pelo formulário')
		dic['gestor'] = st.selectbox('Coordenador', gestores)
		dic['notas de manutenção'] = st_tags(label='Notas de manutenção', text='Pressione enter')
		dic['ordem manutenção'] = st_tags(label='Ordens de manutenção', text='Pressione enter')
		dic['status'] = 'Pendente'
		submitted_ins = st.form_submit_button('Enviar 5 Porquês')

	if submitted_ins:
		caching.clear_cache()
		keys_values = dic.items()
		new_d = {str(key): str(value) for key, value in keys_values}
		for key, value in new_d.items():
			if (value == '') or value == '[]':
				new_d[key] = 'Não informado'
				
		if '@ambev.com.br' in new_d['email responsável']:
			ts = time.time()
			val_documento = new_d['linha'] + new_d['equipamento'].replace(" ", "") + new_d['data'] + new_d['hora']
			#val_documento = new_d['linha'] + '-' + new_d['equipamento'].replace(" ", "") + '-' + str(int(ts))
			doc_ref = db.collection("5porques_2").document(val_documento)
			doc_ref.set(new_d)
			email_gestor = usuarios_fb[usuarios_fb['Nome'] == new_d['gestor']]['Email']
			send_email(str(email_gestor.iloc[0]), 0, val_documento, '', new_d['gatilho'])
		else:
			st.error('Digite e-mail Ambev válido')
				
######################################################################################################
                                           #Main
######################################################################################################

# Carrega dataframe e extrai suas colunas
dados = load_data()
usuarios_fb = load_usuarios()
sap_nv3 = load_sap_nv3()
df_pendencia = load_pendencias()
gestores = list(usuarios_fb[usuarios_fb['Gestor'].str.lower() == 'sim']['Nome'])
nao_gestores = list(usuarios_fb[usuarios_fb['Gestor'].str.lower() != 'sim']['Nome'])
colunas = dados.columns
mes = load_mes()
colunas_mes = mes.columns
#st.write(mes)

# Constantes
equipamentos = []
#gatilhos = [ 'Segurança', '10 minutos', '30 minutos', '1 hora']
linhas = sap_nv3['Linha'].drop_duplicates()
turnos = ['Turno A', 'Turno B', 'Turno C']
tipos = ['Automação', 'Eletricidade', 'Elétrico', 'Falha - Automação', 'Falha - Elétrica', 'Falha - Mecânica', 'Falha - Operacional', 'Mecânica', 'Operacional']
falhas = ['Máquina', 'Mão-de-obra', 'Método', 'Materiais', 'Meio ambiente', 'Medição', 'Outra']
deterioização = ['Forçada', 'Natural', 'Nenhuma']

# Imagem
st.image('Ambev.jpeg')
st.subheader('Aplicação 5-porques')
st.write('Selecione no menu lateral a opção desejada')

# Lista vazia para input dos dados do formulário
dic = {} #dicionario

if func_escolhida == 'Pendências':
	
	st.subheader('Últimas pendências')
	qtd_pendencias = st.slider('Selecione quantas pendencias deseja visualiar', 10)
	st.write(df_pendencia.tail(qtd_pendencias)[['data', 'turno', 'linha', 'equipamento', 'departamento', 'usuario', 'descrição']])
		 
	st.subheader('Inserir pendências')
	st.write('Inserir possíveis 5-Porques para verificação')
	sp2, sp3= st.beta_columns(2)
	list_linhas = list(linhas)
	sap_nv2 = sp2.selectbox('Selecione a linha ', list_linhas)	
	equipamentos = list(sap_nv3[sap_nv3['Linha'] == sap_nv2]['equipamento'])

	with st.form('Form_pend'):
		st1, st2, st3 = st.beta_columns(3)
		dic['data'] = st1.date_input('Data da pendência')
		dic['turno'] = st2.selectbox('Selecione turno', turnos )
		dic['definição do evento'] = st3.selectbox('Definição do Evento ', tipos)
		dic['linha'] = sap_nv2
		dic['equipamento'] = sp3.selectbox('Selecione equipamento', equipamentos)	
		dic['descrição'] = st.text_input('Descreva o ocorrido', "")
		dic['usuario'] = st.text_input('Nome do colaborador que identificou a pendência')
		dic['status'] = 'Pendente'
		submitted_pend = st.form_submit_button('Criar pendência')

	if submitted_pend:
		caching.clear_cache()
		keys_values = dic.items()
		new_d = {str(key): str(value) for key, value in keys_values}
		for key, value in new_d.items():
			if (value == '') or value == '[]':
				new_d[key] = 'Não informado'
				
		ts = time.time()
		val_documento = new_d['linha'] + '-' + new_d['equipamento'].replace(" ", "") + '-' + str(int(ts))
		doc_ref = db.collection("pendencias").document(val_documento)
		doc_ref.set(new_d)
		st.write('Pendência criada com sucesso')
	st.subheader('Integração com MES')
	uploaded_file = st.file_uploader("Selecione o arquivo Excel para upload")
	if uploaded_file is not None:
		up_mes = upload_mes(uploaded_file, tipos)
		st.write(up_mes)
	mes = load_mes()
	st.write(mes)
	
	#st.write(mes.groupby(['Linha', 'Equipamento']).count_values())
	
if func_escolhida == 'Inserir':
	st.subheader('Formulário 5-porques')
	formulario(linhas)

if func_escolhida == 'Consultar':
	st.subheader('Configure as opções de filtro')
	st.text('Selecione a data')
	col1, col2 = st.beta_columns(2)
	inicio_filtro = col1.date_input("Início")
	fim_filtro = col2.date_input("Fim")
	filtrado = (dados[(dados['data'] >= inicio_filtro) & (dados['data'] <= fim_filtro)]) 
	
	list_resp = list(filtrado['responsável identificação'].drop_duplicates())
	list_resp.append('todos') 
	responsavel = st.selectbox("Selecione o responsável", list_resp, list_resp.index('todos'))
	if responsavel == 'todos':
		pass
	elif responsavel is not None and (str(responsavel) != 'nan'):
		filtrado = filtrado[filtrado['responsável identificação'] == responsavel]
		
	list_gestor = list(filtrado['gestor'].drop_duplicates())
	list_gestor.append('todos')  
	gestor = st.selectbox("Selecione o gestor", list_gestor, list_gestor.index('todos'))
	if gestor == 'todos':
		pass
	elif gestor is not None and (str(gestor) != 'nan'):
		filtrado = filtrado[filtrado['gestor'] == gestor]	
	
	list_status = list(filtrado['status'].drop_duplicates())
	list_status.append('todos') 
	status = st.selectbox("Selecione o status", list_status, list_status.index('todos'))
	if status == 'todos':
		pass
	elif status is not None and (str(status) != 'nan'):
		filtrado = filtrado[filtrado['status'] == status]	
	
	st.write(filtrado[['data', 'document', 'gestor', 'status','responsável identificação', 'turno', 'linha', 'equipamento']])
	st.markdown(get_table_download_link(filtrado), unsafe_allow_html=True)
	indice_doc = st.multiselect('Selecione a ocorrência', filtrado['document'].tolist())
	
	for index, row in filtrado.iterrows():
		if row['document'] in indice_doc:
			st.subheader('Ocorrência ' + str(row['document']))
			func_validar(index, row, indice_doc)
			        
if func_escolhida == 'Estatísticas':
	st.subheader("Estatísticas 5-Porques")
	#graf1, graf2, graf3 = st.beta_columns(3)
	#variavel =  st.selectbox('Selecione o item para análise', colunas)
	#fig1 = px.histogram(dados, x='turno')
	#graf1.write(fig1)
	
	#fig2 = px.histogram(dados, x='data', nbins=31)
	#graf2.write(fig2)
	
	#line_equip = dados['linha'].astype(str) + dados['equipamento'].astype(str)
	#fig3 = px.histogram(line_equip)
	
	#st.subheader("Estatísticas MES")
	#variavel_mes =  st.selectbox('Selecione o item para análise', colunas_mes)
	#fig_mes = px.histogram(mes, x=variavel_mes)
	#st.write(fig_mes)

	fig = make_subplots(rows=1, cols=3)
	
#fig.add_trace(go.Histogram(x=x0))
#data=[go.Histogram(x=x)
	st.text('Selecione a data')
	col_1, col_2 = st.beta_columns(2)
	inicio_filt = col_1.date_input("Início")
	fim_filt = col_2.date_input("Fim")
	filtrado_5pq = (dados[(dados['data'] >= inicio_filt) & (dados['data'] <= fim_filt)]) 
	filtrado_mes = (mes[(mes['Data'] >= inicio_filt) & (mes['Data'] <= fim_filt)]) 

      
	fig.add_trace(
	    go.Histogram(x=filtrado_5pq['data'], nbinsy=31),
	    row=1, col=1
	)

	fig.add_trace(
	    go.Histogram(x=filtrado_mes['Data'], nbinsy=31),
	    row=1, col=1
	)
	fig.add_trace(
	    go.Histogram(x=filtrado_5pq['turno']),
	    row=1, col=2
	)

	fig.add_trace(
	    go.Histogram(x=filtrado_mes['Turno']),
	    row=1, col=2
	)
	mes_produtivo = filtrado_5pq['linha'].astype(str) + filtrado_5pq['equipamento'].astype(str)
	fig.add_trace(
	    go.Histogram(x=mes_produtivo),
	    row=1, col=3
	)

	fig.add_trace(
	    go.Histogram(x=filtrado_mes['Ponto Produtivo']),
	    row=1, col=3
	)

	fig.update_layout(height=300, width=1200, title_text="5-Porques vs MES")
	st.write(fig)




