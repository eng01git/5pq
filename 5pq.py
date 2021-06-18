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
import datetime
import time
from datetime import  date
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

st.write('teste')

# Pega as configurações do banco do segredo
key_dict = json.loads(st.secrets["textkey"])
creds = service_account.Credentials.from_service_account_info(key_dict)

# Seleciona o projeto
db = firestore.Client(credentials=creds, project="st-5why")
doc_ref = db.collection(u'5porques')

# Link do arquivo com os dados
DATA_URL = "data.csv"

######################################################################################################
				     #Definição da sidebar
######################################################################################################

fig1s, fig2s, fig3s = st.sidebar.beta_columns([1,2,1])
fig1s.write('')
fig2s.image('latas minas.png', width=150)
fig3s.write('')
st.sidebar.title("Menu 5-Porques")
func_escolhida = st.sidebar.radio('Selecione a opção desejada',('Visibilidade', 'Inserir', 'Consultar' , 'Gerenciamento das ações', 'Suporte Engenharia'), index=0)

######################################################################################################
                               #Função para leitura do banco (Firebase)
######################################################################################################
# Efetua a leitura de todos os documentos presentes no banco e passa para um dataframe pandas
# Função para carregar os dados do firebase (utiliza cache para agilizar a aplicação)
@st.cache
def load_data():
	data = pd.read_csv(DATA_URL)
	
	# Define o caminho da coleção do firebase
	posts_ref = db.collection("5porques_2")	
	
	# Busca todos os documentos presentes na coleção e salva num dataframe
	for doc in posts_ref.stream():
		dicionario = doc.to_dict()
		dicionario['document'] = doc.id
		data = data.append(dicionario, ignore_index=True)
	
	# Formata as colunas de data e hora para possibilitar filtros
	data['data'] = pd.to_datetime(data['data']).dt.date
	data['hora'] = pd.to_datetime(data['hora']).dt.time
	return data

@st.cache
def load_mes():
	# Cria dicionário vazio
	dicionario = {}
	
	# Define o caminho da coleção do firebase
	posts_ref = db.collection("MES_data")
	
	# Busca todos os documentos presentes na coleção e salva num dicionário
	for doc in posts_ref.stream():
		dic_auxiliar = doc.to_dict()
		dicionario[dic_auxiliar['documento']] = dic_auxiliar
	
	# Ajusta o dicionário para um dataframe
	mes_df = pd.DataFrame.from_dict(dicionario)
	mes_df = mes_df.T
	mes_df.reset_index(inplace=True)
	mes_df.drop('index', axis=1, inplace=True)
	
	# Lista e ordena as colunas do dataframe
	lista_colunas = ['Linha', 'Data', 'Hora', 
			 'Tempo', 'Micro/Macro', 'Definição do Evento', 'Nome', 
			 'Equipamento','Ponto Produtivo', 'SubConjunto', 'Componente', 'Modo de Falha - Sintoma', 
			 'Descrição', 'Lote', 'Resultante', 'FluxoProduto', 'FluxoIntervalo', 'Turno', 'Gargalo', 'FiltroExterna', 'documento']
	mes_df = mes_df.reindex(columns=lista_colunas)
	
	# Formata as colunas de data e hora para possibilitar filtros
	mes_df['Data'] = pd.to_datetime(mes_df['Data']).dt.date
	mes_df['Hora'] = pd.to_datetime(mes_df['Hora']).dt.time
	
	# Adequa os valores dos turnos
	#mes_df['Turno'] = mes_df['Turno'].map({'Morning': 'Turno A', 'Afternoon': 'Turno B', 'Evening': 'Turno C'})
	mes_df.loc[(mes_df['Hora'] >= datetime.time(23, 0, 0)) | (mes_df['Hora'] < datetime.time(7, 0, 0)), 'Turno'] = 'Turno A'
	mes_df.loc[(mes_df['Hora'] >= datetime.time(7, 0, 0)) & (mes_df['Hora'] < datetime.time(15, 0, 0)), 'Turno'] = 'Turno B'
	mes_df.loc[(mes_df['Hora'] >= datetime.time(15, 0, 0)) & (mes_df['Hora'] < datetime.time(23, 0, 0)), 'Turno'] = 'Turno C'

	# Ordena os valores pela data
	mes_df.sort_values(by=['Data'], inplace=True)
	return mes_df

def upload_mes(uploaded_file, tipos):
	# Leitura dos dados do arquivo excel
	try:
		data = pd.read_excel(uploaded_file, sheet_name='Parada')
		
		# Filtrando os dados (tempo maior que 30 e eventos incluídos em tipo)
		data = data[(data['Tempo'] > 30.0)]
		data = data[data['Definição do Evento'].isin(tipos)]

		# Ajuste da variável de data
		data['Data'] = data['Data'].dt.date

		# Criação do nome do documento
		data['documento'] = data['Linha'].astype(str) + data['Equipamento'].astype(str) + data['Data'].astype(str) + data['Hora'].astype(str)

		# Cria dicionário vazio
		dicionario = {}

		# Define o caminho da coleção do firebase
		posts_ref = db.collection("MES_data")

		# Busca todos os documentos presentes na coleção e salva num dicionário
		for doc in posts_ref.stream():
			dic_auxiliar = doc.to_dict()
			dicionario[dic_auxiliar['documento']] = dic_auxiliar

		# Filtra os valores presentes no arquivo e não presentes na base dados
		to_include = data[~data['documento'].isin(dicionario.keys())]

		# Se houver variáveis a serem incluídas e faz a inclusão
		if to_include.shape[0] > 0 :
			batch = db.batch()
			for index, row in to_include.iterrows():
				ref = db.collection('MES_data').document(row['documento'])
				row_string = row.astype(str)
				batch.set(ref, row_string.to_dict())
			batch.commit()	
			
		# Limpa cache
		caching.clear_cache()		
		return to_include
	except:
		st.error('Arquivo não compatível com exportação do MES')
		return None

# Efetua a leitura dos dados dos usuários no banco
@st.cache
def load_usuarios():
	# Define as colunas do dataframe
	data_user = pd.DataFrame(columns=['Nome', 'Email', 'Gestor', 'Codigo'])
	
	# Define o caminho da coleção do firebase
	posts_ref = db.collection("Users")	
	
	# Busca todos os documentos presentes na coleção e salva num dataframe
	for doc in posts_ref.stream():
		dicionario = doc.to_dict()
		dicionario['document'] = doc.id
		data_user = data_user.append(dicionario, ignore_index=True)
	return data_user

# Efetua a leitura das pendencias no banco
@st.cache
def load_pendencias():
	# Define as colunas do dataframe
	data = pd.DataFrame(columns=['data', 'turno', 'linha', 'equipamento', 'departamento', 'usuario', 'descrição'])
	
	# Define o caminho da coleção do firebase
	posts_ref = db.collection("pendencias")	
	
	# Busca todos os documentos presentes na coleção e salva num dataframe
	for doc in posts_ref.stream():
		dicionario = doc.to_dict()
		dicionario['document'] = doc.id
		data = data.append(dicionario, ignore_index=True)
	return data

# Efetua a leitura dos dados das linhas e dos equipamentos
@st.cache
def load_sap_nv3():
	# Efetua a leitura dos dados do arquivo csv
	data_sapnv3 = pd.read_csv('SAP_nivel3.csv', sep=';')
	return data_sapnv3

# Efetua a escrita das ações no banco de dados
def write_acoes(acoes, documento, gestor):
	
	# Define o caminho da coleção do firebase
	posts_ref = db.collection("acoes")	
	
	# Lista e dicionario vazio
	acoes_firebase = []
	dic_to_firebase = {}
	
	# Busca todos os documentos presentes na coleção e salva num dataframe
	for doc in posts_ref.stream():
		acoes_firebase.append(doc.id)
		
	index = 0
	
	# Itera sobre todas as ações do 5-Porques
	for i in acoes:
		
		# Separa a string
		lista = i.split(";;")
		
		# Define o nome do documento a ser armazenado na base de dados
		chave = str(documento) + '_' + str(index)
		
		# Verifica se a ação já consta no banco de dados
		if chave not in acoes_firebase:	
			
			# Definição da ação com alteração do status para em aberto
			dic_to_firebase[chave] = {'Ação': lista[0],
						  'Dono': lista[1],
						  'Prazo': lista[2],
						  'Numero da ação': index,
						  'Numero do 5-Porques': documento,
						  'Status': 'Em aberto',
						  'Gestor': gestor,
						  'E-mail': 'Não enviado',
						  'Editor': '',
						  'Data': ''
						 }		
			db.collection("acoes").document(chave).set(dic_to_firebase[chave],merge=True)
			
			# Envia email para o dono da acao 
			remetente = usuarios_fb.loc[usuarios_fb['Nome'] == lista[1], 'Email'].to_list()
			send_email(remetente[0], 6, lista[2], lista[0], 0)
			time.sleep(1)
			#send_email(to, atividade, documento, comentario, gatilho):
			
		else:
			
			# Caso a ação já esteja no banco de dados, ela é modificada mas seu status não é alterado
			dic_to_firebase[chave] = {'Ação': lista[0],
						  'Dono': lista[1],
						  'Prazo': lista[2],
						  'Numero da ação': index,
						  'Numero do 5-Porques': documento,
						  'Gestor': gestor,
						  'E-mail': 'Não enviado'
						 }		
			db.collection("acoes").document(chave).set(dic_to_firebase[chave],merge=True)
		index += 1

# Efetua a leitura das ações no banco de dados
@st.cache
def read_acao():
	# Cria dicionário vazio
	dicionario_acao = {}
	
	# Define o caminho da coleção do firebase
	posts_ref = db.collection("acoes")
	
	# Busca todos os documentos presentes na coleção e salva num dicionário
	for doc in posts_ref.stream():
		dic_auxiliar = doc.to_dict()
		dict_key = dic_auxiliar['Numero do 5-Porques'] + '_' + str(dic_auxiliar['Numero da ação'])
		dicionario_acao[dict_key] = dic_auxiliar
	
	# Ajusta o dicionário para um dataframe
	acao_df = pd.DataFrame.from_dict(dicionario_acao)
	acao_df = acao_df.T
	acao_df.reset_index(inplace=True)
	acao_df.drop('index', axis=1, inplace=True)
	
	# Lista e ordena as colunas do dataframe
	lista_colunas = ['Ação', 'Dono', 'Prazo','Status', 'Gestor', 'E-mail', 
			 'Numero do 5-Porques',  'Numero da ação', 'Editor', 'Data']
	acao_df = acao_df.reindex(columns=lista_colunas)
	
	# Formata a coluna de prazo para possibilitar filtros
	acao_df['Prazo'] = pd.to_datetime(acao_df['Prazo']).dt.date
	
	# Ordena os valores pelo prazo
	acao_df.sort_values(by=['Prazo'], inplace=True)
	return acao_df

# Grava a edição das ações no banco
def gravar_acao_edit(row):
	ea_chave = str(row['Numero do 5-Porques']) + '_' + str(row['Numero da ação'])
	row_string = row.astype(str)
	db.collection("acoes").document(ea_chave).set(row_string.to_dict(),merge=True)
	caching.clear_cache()

# Ainda não definida
def editar_acao(row):
	
	
	gravar_acao_edit(row)

######################################################################################################
                                           #Função para enviar email
######################################################################################################
# Recebe como parâmetros destinatário e um código de atividade para o envio
# O email está configurado por parâmetros presentes no streamlit share (secrets)
def send_email(to, atividade, documento, comentario, gatilho):
	
	# Configura quem vai enviar o email
	gmail_user = st.secrets["email"]
	gmail_password = st.secrets["senha"]
	sent_from = gmail_user
	
	# Cria os campos que estarão no e-mail
	from_ = 'Ambev 5 Porques'
	subject = ""
	body = ''
	atividade = int(atividade)
	
	# Verifica o código da atividade e edita a mensagem (criação, retificação, aprovação ou reproavação de 5-Porques
	# Criação
	if atividade == 0:
		body = "Olá, foi gerada um novo 5-Porques, acesse a plataforma para avaliar.\nhttps://share.streamlit.io/eng01git/5pq/main/5pq.py\n\nAtenciosamente, \nAmbev 5-Porques"
		subject = """Gerado 5-Porques %s""" % (documento)
		
	# Retificação
	elif atividade == 1:
		body = "Olá, o responsável retificou 5-Porques, acesse a plataforma para reavaliar.\nhttps://share.streamlit.io/eng01git/5pq/main/5pq.py\n\nAtenciosamente, \nAmbev 5-Porques"
		subject = """Retificado 5-Porques %s""" % (documento)
		
	# Aprovação
	elif atividade == 2:
		body = """Olá, o gestor aprovou 5-Porques.\n\n%s \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario)
		subject = """Aprovado 5-Porques %s""" % (documento)	
		
	# Reprovação
	elif atividade == 3:
		body = """Olá, o gestor reprovou 5-Porques, acesse a plataforma para retificar.\nhttps://share.streamlit.io/eng01git/5pq/main/5pq.py \n\n Comentario do gestor: \n\n%s  \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario)
		subject = """Reprovado 5-Porques %s""" % (documento)
	
	# Mensagem pro suporte
	elif atividade == 4:
		body = """Olá, segue mensagem enviada ao suporte.\n\n%s \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario)
		subject = 'Suporte 5-Porques'
		
	# Acao atrasada
	elif atividade == 5:
		body = """Olá, a ação "%s" esta atrasada. \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario)
		subject = 'Ação atrasada'
		
	# acao criada
	elif atividade == 6:
		body = """Olá, a ação "%s" foi criada e deve ser finalizada ate %s. \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario, documento)
		subject = 'Ação criada'
		
	# acao concluida
	elif atividade == 7:
		body = """Olá, a ação "%s" foi finalizada hoje por %s. \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario, documento)
		subject = 'Ação concluida'
		
	# acao cancelada
	elif atividade == 8:
		body = """Olá, a ação "%s" foi cancelada hoje por %s. \n\nAtenciosamente, \nAmbev 5-Porques""" %(comentario, documento)
		subject = 'Ação cancelada'
	
	# Transforma o remetente em lista
	list_to = [to]
	
	# Verifica se precisa mandar o e-mail para a engenharia
	if int(gatilho) > 60:	
		
		list_to.append('99814840@ambev.com.br')
		list_to.append('99814849@ambev.com.br')
	
	# Monta a mensagem
	email_text = """From: %s\nTo: %s\nSubject: %s\n\n%s
	""" % (from_, list_to, subject, body)
	
	# Envia a mensagem
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
# download de csv		
def download(df):
	"""Generates a link allowing the data in a given panda dataframe to be downloaded
	in:  dataframe
	out: href string
	"""
	csv = df.to_csv(index=False)
	b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
	href = f'<a href="data:file/csv;base64,{b64}">Download dos dados como csv</a>'
	return href

# Gera arquivo excel
def to_excel(df):
	output = BytesIO()
	writer = pd.ExcelWriter(output, engine='xlsxwriter')
	df.to_excel(writer, sheet_name='Sheet1')
	writer.save()
	processed_data = output.getvalue()
	return processed_data

# Gera o link para o download do excel
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
	
	# Verifica se o documento deve ser exibido
	if row['document'] in indice:
		
		# Checkbox para habilitar edição
		editar = st.checkbox('Editar 5-Porques ' + str(row['document']))
		
		# Etapa de avaliação do 5-Porque
		if not editar:
			
			# Mostra os dados do 5-Porque selecionado
			st.table(row)
			st.subheader('Avaliação do 5-Porques')
			
			# Verifica código do gestor
			codigo_gestor = st.text_input('Inserir código do gestor' + ' (' + str(index) + ')', type='password')
			
			# Comentário do Gestor
			comentario = st.text_input('Envie um comentário sobre 5-Porques' + ' (' + str(index) + '):',"")		
			
			# Botões para avaliação do 5-Porque
			bt1, bt2 = st.beta_columns(2)
			aprovar = bt1.button('Aprovar 5-Porques ' + '(' + str(index) + ')')
			reprovar = bt2.button('Reprovar 5-Porques ' + '(' + str(index) + ')')
			st.subheader('Exportar 5-Porques')	
			
			#Download do documento selecionado
			export = filtrado[filtrado['document'] == row['document']]
			st.markdown(get_table_download_link(export), unsafe_allow_html=True)
			
			# Aprova 5-Poque
			if aprovar:
				if codigo_gestor == 'GestorAmbev':
					caching.clear_cache()
					att_verificado = {}
					att_verificado['status'] = 'Aprovado'
					db.collection("5porques_2").document(row['document']).update(att_verificado)
					send_email(row['email responsável'], 2, str(row['document']), comentario, 0)
				else:
					st.error('Código do gestor incorreto')

			# Reprova 5-Porque
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
		# Etapa de edição (Preenchimento do formulário)
		else:
			# Ler o número do documento
			documento = str(row['document'])
			
			# Transforma linha do pandas em dicionário
			doc = row.to_dict()
			
			# Organiza itens na mesma linha
			sp2, sp3, st0, acoes = st.beta_columns(4)
			
			# Lista as linhas da planta e depoois lista os equipamentos em função da linha escolhida
			list_linhas = list(linhas)
			sap_nv2 = sp2.selectbox('Selecione a linha' + ' (' + str(index) + '):', list_linhas, list_linhas.index(doc['linha']))
			equipamentos = list(sap_nv3[sap_nv3['Linha'] == sap_nv2]['equipamento'])
			if sap_nv2 != doc['linha']:
				equipamento_ant = 0
			else:
				equipamento_ant = equipamentos.index(doc['equipamento'])	
			
			# Formulário
			with st.form('Form_edit' + str(index)):
				st1, st2, st3, st4 = st.beta_columns(4)
				dic['data'] = st1.date_input('Data da ocorrência' + ' (' + str(index) + '):', doc['data'])
				dic['turno'] = st2.selectbox('Selecione o turno' + ' (' + str(index) + '):', turnos, turnos.index(doc['turno']))
				dic['hora'] = st3.time_input('Selecione o horário' + ' (' + str(index) + '):', value=doc['hora'])
				dic['definição do evento'] = st4.selectbox('Definição do Evento' + ' (' + str(index) + '):', tipos, tipos.index(doc['definição do evento']))
				dic['linha'] = sap_nv2
				dic['equipamento'] = sp3.selectbox('Selecione o equipamento' + ' (' + str(index) + '):', equipamentos, equipamento_ant)
				dic['gatilho'] = st0.number_input('Gatilho em minutos (mínimo 30 min)' + ' (' + str(index) + '):', value=int(doc['gatilho']), min_value=30)
				dic['quantidade de ações'] = acoes.number_input('Quantidade de ações geradas', min_value=1, value=int(doc['quantidade de ações']), max_value=10)
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
				
				# Lista as ações (há um tratamento pois todas as ações estão numa mesma string)
				lista = doc['ações'].replace('[', '').replace(']', '').split("',")
				dict_acoes = []
				_index = 0
				
				# Disponibiliza todas as ações previamente preenchidas para edição
				for i in lista:
					i = i.lstrip().replace("'",'')
					array = i.split(';;')
					ac, do, pr = st.beta_columns([3,2,1])
					_ação = ac.text_input('Ação' + ' (' + str(index) + ')(' + str(_index) + '):', value=array[0]) 
					_dono = do.selectbox('Dono' + ' (' + str(index) + ')(' + str(_index) + '):', nao_gestores, nao_gestores.index(array[1])) 
					_prazo = pr.date_input('Prazo' + ' (' + str(index) + ')(' + str(_index) + '):', value=date.fromisoformat(array[2]))
					_index += 1
					dict_acoes.append(str(_ação) + ';;' + str(_dono) + ';;' + str(_prazo))
					if _index == dic['quantidade de ações']:
						break
				
				# Verifica se foram acrescentadas novas ações e disponibiliza os campos para edição
				if dic['quantidade de ações'] > len(lista):
					for i in list(range(dic['quantidade de ações'] - len(lista))):
						ac, do, pr = st.beta_columns([3,2,1])
						_ação = ac.text_input('Ação' + ' (' + str(index) + ')(' + str(i + len(lista)) + '):', "") 
						_dono = do.selectbox('Dono' + ' (' + str(index) + ')(' + str(i + len(lista)) + '):', nao_gestores, nao_gestores.index(array[1])) 
						_prazo = pr.date_input('Prazo' + ' (' + str(index) + ')(' + str(i + len(lista)) + '):')
						dict_acoes.append(str(_ação) + ';;' + str(_dono) + ';;' + str(_prazo))
						
				dic['ações'] = dict_acoes
				dic['notas de manutenção'] = st_tags(label=('Notas de manutenção' + ' (' + str(index) + '):'), text='Pressione enter', value=doc['notas de manutenção'].replace(']', '').replace('[','').replace("'",'').split(','), key=[0])
				dic['ordem manutenção'] = st_tags(label=('Ordem de manutenção' + ' (' + str(index) + '):'), text='Pressione enter', value=doc['ordem manutenção'].replace(']', '').replace('[','').replace("'",'').split(','),  key=[1])
				dic['status'] = 'Retificado'
				st8, st9 = st.beta_columns(2)
				dic['responsável identificação'] = st8.selectbox('Responsável pela identificação' + ' (' + str(index) + '):', nao_gestores, nao_gestores.index(doc['responsável identificação']))
				dic['responsável reparo'] = st9.selectbox('Responsável pela correção' + ' (' + str(index) + '):', nao_gestores, nao_gestores.index(doc['responsável reparo']))
				dic['email responsável'] = st.text_input('E-mail do responsável pelo formulário' + ' (' + str(index) + '):', value=doc['email responsável'])
				dic['gestor'] = st.selectbox('Coordenador' + ' (' + str(index) + '):', gestores, gestores.index(doc['gestor']))
				submitted_edit = st.form_submit_button('Editar 5 Porquês' + ' (' + str(index) + '):')
			
			# Envio do formulario
			if submitted_edit:
				
				# Escreve as acoes em um banco
				write_acoes(dic['ações'], documento, dic['gestor'])
				
				# Transforma dados do formulário em um dicionário
				keys_values = dic.items()
				new_d = {str(key): str(value) for key, value in keys_values}
				
				# Verifica campos não preenchidos e os modifica
				for key, value in new_d.items():
					if (value == '') or value == '[]':
						new_d[key] = 'Não informado'
						
				#verifica o campo de e-mail (é obrigatório o preenchimento)
				if '@ambev.com.br' in new_d['email responsável']:
					
					# Define o nome do documento a ser editado
					db.collection("5porques_2").document(documento).set(new_d,merge=True)
					
					# Escreve as acoes em um banco
					write_acoes(dic['ações'], documento, dic['gestor'])
					
					editar = False
					
					# Envia e-mail para gestor
					email_gestor = usuarios_fb[usuarios_fb['Nome'] == new_d['gestor']]['Email']
					send_email(str(email_gestor.iloc[0]), 1, documento, '', new_d['gatilho'])
					
					# Limpa cache
					caching.clear_cache()
				else:
					st.error('Por favor inserir e-mail Ambev válido')
					
######################################################################################################
                              #Formulário para inclusão de 5-Porques
######################################################################################################

def formulario(linhas):
	
	# Preenchimento do formulário
	sp2, sp3, st0, acoes = st.beta_columns(4)
	
	# Lista as linhas da planta e depoois lista os equipamentos em função da linha escolhid
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
		dic['quantidade de ações'] = acoes.number_input('Quantidade de ações geradas', min_value=1, max_value=10)
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
		
		# Cria uma lista para armazenar as ações
		dict_acoes = []
		
		# Cria os campos para preenchimento das ações em função da quantidade selecionada
		for i in list(range(dic['quantidade de ações'])):
			ac, do, pr = st.beta_columns([3,2,1])
			_ação = ac.text_input('Ação (' + str(i) + '):', "") 
			_dono = do.selectbox('Dono (' + str(i) + '):', nao_gestores)
			_prazo = pr.date_input('Prazo (' + str(i) + '):')
			dict_acoes.append(str(_ação) + ';;' + str(_dono) + ';;' + str(_prazo))			
			      
		dic['ações'] = dict_acoes
		st8, st9 = st.beta_columns(2)
		dic['notas de manutenção'] = st_tags(label='Notas de manutenção', text='Pressione enter')
		dic['ordem manutenção'] = st_tags(label='Ordens de manutenção', text='Pressione enter')
		dic['status'] = 'Pendente'
		dic['responsável identificação'] = st8.selectbox('Responsável pela identificação',nao_gestores)
		dic['responsável reparo'] = st9.selectbox('Responsável pela correção',nao_gestores)
		dic['email responsável'] = st.text_input('E-mail do responsável pelo formulário')
		dic['gestor'] = st.selectbox('Coordenador', gestores)
		submitted_ins = st.form_submit_button('Enviar 5 Porquês')
		
	# Envio do formulário
	if submitted_ins:
		# Limpa cache
		caching.clear_cache()
		
		# Transforma dados do formulário em um dicionário
		keys_values = dic.items()
		new_d = {str(key): str(value) for key, value in keys_values}

		# Verifica campos não preenchidos e os modifica
		for key, value in new_d.items():
			if (value == '') or value == '[]':
				new_d[key] = 'Não informado'
		
		#verifica o campo de e-mail (é obrigatório o preenchimento)
		if '@ambev.com.br' in new_d['email responsável']:
			
			# Dfine o nome do documento a ser armazenado no banco
			val_documento = new_d['linha'] + new_d['equipamento'].replace(" ", "") + new_d['data'] + new_d['hora']
			
			# Escreve as acoes em um banco
			write_acoes(dic['ações'], val_documento, dic['gestor'])
			
			# Armazena 5-Poruqes no banco
			doc_ref = db.collection("5porques_2").document(val_documento)
			doc_ref.set(new_d)
			
			# Envia e-mail para gestor
			email_gestor = usuarios_fb[usuarios_fb['Nome'] == new_d['gestor']]['Email']
			send_email(str(email_gestor.iloc[0]), 0, val_documento, '', new_d['gatilho'])
		else:
			st.error('Digite e-mail Ambev válido')
				
######################################################################################################
                                           #Main
######################################################################################################

if __name__ == '__main__':
	# Carrega dados do firebase
	dados = load_data()
	usuarios_fb = load_usuarios()
	sap_nv3 = load_sap_nv3()
	df_pendencia = load_pendencias()
	mes = load_mes()
	
	# Separa os gestores e não-gestores
	gestores = list(usuarios_fb[usuarios_fb['Gestor'].str.lower() == 'sim']['Nome'])
	nao_gestores = list(usuarios_fb[usuarios_fb['Gestor'].str.lower() != 'sim']['Nome'])
	
	# Separa as colunas dos dataframes
	colunas = dados.columns
	colunas_mes = mes.columns

	# Constantes
	equipamentos = []
	linhas = sap_nv3['Linha'].drop_duplicates()
	turnos = ['Turno A', 'Turno B', 'Turno C']
	tipos = ['Automação', 'Eletricidade', 'Elétrico', 'Falha - Automação', 'Falha - Elétrica', 'Falha - Mecânica', 'Falha - Operacional', 'Mecânica', 'Operacional']
	falhas = ['Máquina', 'Mão-de-obra', 'Método', 'Materiais', 'Meio ambiente', 'Medição', 'Outra']
	deterioização = ['Forçada', 'Natural', 'Nenhuma']

	# Imagem
	col1_, col2_, col3_ = st.beta_columns([1,1,1])
	col1_.write('')
	col2_.image('Ambev.jpeg', width=250)
	col3_.write('')

	# Lista vazia para input dos dados do formulário
	dic = {} #dicionario

	# Função desabilitda, remover
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

	if func_escolhida == 'Inserir':
		
		# Chama função de formulário
		st.subheader('Formulário 5-porques')
		formulario(linhas)

	if func_escolhida == 'Consultar':
		st.subheader('Configure as opções de filtro')
		
		# Filtro para datas
		st.text('Selecione a data')
		col1, col2, col3 = st.beta_columns(3)
		user_gest, user_resp = st.beta_columns(2)
		inicio_filtro = col1.date_input("Início")
		fim_filtro = col2.date_input("Fim")
		filtrado = (dados[(dados['data'] >= inicio_filtro) & (dados['data'] <= fim_filtro)]) 

		# Gera lista dos responsáveis
		list_resp = list(filtrado['responsável identificação'].drop_duplicates())
		list_resp.append('todos') 
		responsavel = user_resp.selectbox("Selecione o responsável", list_resp, list_resp.index('todos'))
		
		# Inicia o filtro com todos
		if responsavel == 'todos':
			pass
		elif responsavel is not None and (str(responsavel) != 'nan'):
			filtrado = filtrado[filtrado['responsável identificação'] == responsavel]

		# Gera lista dos gestor	
		list_gestor = list(filtrado['gestor'].drop_duplicates())
		list_gestor.append('todos')  
		gestor = user_gest.selectbox("Selecione o gestor", list_gestor, list_gestor.index('todos'))
		
		# Inicia o filtro com todos
		if gestor == 'todos':
			pass
		elif gestor is not None and (str(gestor) != 'nan'):
			filtrado = filtrado[filtrado['gestor'] == gestor]	

		# Gera lista dos status	
		list_status = list(filtrado['status'].drop_duplicates())
		list_status.append('todos') 
		status = col3.selectbox("Selecione o status", list_status, list_status.index('todos'))
		
		# Inicia o filtro com todos
		if status == 'todos':
			pass
		elif status is not None and (str(status) != 'nan'):
			filtrado = filtrado[filtrado['status'] == status]	

		# Lista o resultado dos filtros	
		st.write(filtrado[['data', 'document', 'gestor', 'status','responsável identificação', 'turno', 'linha', 'equipamento']])
		
		# Disponibiliza os dados filtrados para download
		st.markdown(get_table_download_link(filtrado), unsafe_allow_html=True)
		
		# Campo para selecionar um ou mais 5-Porques para análise
		indice_doc = st.multiselect('Selecione o 5-Porques', filtrado['document'].tolist())
		
		# Verifica quais 5 porques foram selecionados e chama a função de validação/edição para cada item selecionado
		for index, row in filtrado.iterrows():
			if row['document'] in indice_doc:
				st.subheader('Ocorrência ' + str(row['document']))
				func_validar(index, row, indice_doc)
	
	# Gera estatísticas sobre os 5-Porques comparando com o MES
	if func_escolhida == 'Visibilidade':
		st.subheader("Visibilidade 5-Porques vs MES")
		st_grafico = st.empty()
		col_1, col_2 = st.beta_columns(2)

		#seleciona a data que mostra os 50 primeiros itens do MES (carrega com data e já mostra valores)
		data_row = mes.shape[0] - 50
		data_default = mes.iloc[data_row, 1]
		
		#filtra os dados com base na data
		inicio_filt = col_1.date_input("Data inicial", value=data_default)
		fim_filt = col_2.date_input("Data final")
		filtrado_5pq = (dados[(dados['data'] >= inicio_filt) & (dados['data'] <= fim_filt)]) 
		filtrado_mes = (mes[(mes['Data'] >= inicio_filt) & (mes['Data'] <= fim_filt)]) 

		# Criação dos gráficos (5 subplots)
		fig = make_subplots(rows=1, 
				    cols=5,
				    subplot_titles=("Datas", "Turnos", "Equipamentos", 'Linhas', '60 min ou mais?'),
				    column_widths=[0.2, 0.2, 0.4, 0.1, 0.1]
				   )
		
		# verifica se há dados de 5-Porques para o período
		try:
			# Histograma da data
			fig.add_trace(go.Histogram(x=filtrado_5pq['data'], marker=dict(color='rgba(12, 50, 196, 0.6)')), row=1, col=1)
			# Histograma do turno
			fig.add_trace(go.Histogram(x=filtrado_5pq['turno'], marker=dict(color='rgba(12, 50, 196, 0.6)')), row=1, col=2)
			# Histograma da equipamento (ponto produtivo)
			mes_produtivo = filtrado_5pq['linha'].astype(str) + '-' + filtrado_5pq['equipamento'].astype(str)
			fig.add_trace(go.Histogram(x=mes_produtivo, marker=dict(color='rgba(12, 50, 196, 0.6)')), row=1, col=3)
			# Histograma das linhas
			filtrado_5pq['linha'] = filtrado_5pq['linha'].str.replace('0','').str.replace('M-', '')
			fig.add_trace(go.Histogram(x=filtrado_5pq['linha'], marker=dict(color='rgba(12, 50, 196, 0.6)')), row=1, col=4)
			# Lógica para mostrar quantos possuem mais de 60 minutos
			filtrado_5pq.loc[filtrado_5pq['gatilho'].astype(float) > 60, '60minutos'] = 'Sim'
			filtrado_5pq.loc[filtrado_5pq['gatilho'].astype(float) <= 60, '60minutos'] = 'Não'
			# Histograma dos 60 minutos
			fig.add_trace(go.Histogram(x=filtrado_5pq['60minutos'], marker=dict(color='rgba(12, 50, 196, 0.6)')), row=1, col=5)
		except:
			st.error('Não há dados de 5-Porques nesse período')
			
		# Verifica se há dados do MES para o período
		try:	
			# Histograma da data
			fig.add_trace(go.Histogram(x=filtrado_mes['Data'], marker=dict(color='grey')), row=1, col=1)
			# Histograma do turno
			fig.add_trace(go.Histogram(x=filtrado_mes['Turno'], marker=dict(color='grey')), row=1, col=2)
			# Histograma da equipamento (ponto produtivo)
			fig.add_trace(go.Histogram(x=filtrado_mes['Ponto Produtivo'].sort_values(), marker=dict(color='grey')), row=1, col=3)
			# Histograma das linhas
			fig.add_trace(go.Histogram(x=filtrado_mes['Linha'], marker=dict(color='grey')), row=1, col=4)
			# Lógica para mostrar quantos possuem mais de 60 minutos
			filtrado_mes.loc[filtrado_mes['Tempo'].astype(float) > 60, '60minutos'] = 'Sim'
			filtrado_mes.loc[filtrado_mes['Tempo'].astype(float) <= 60, '60minutos'] = 'Não'
			# Histograma dos 60 minutos
			fig.add_trace(go.Histogram(x=filtrado_mes['60minutos'], marker=dict(color='grey')), row=1, col=5)
		except:
			st.error('Não há dados do MES nesse período')	

		# Configura figura e plota o gráfico	
		fig.update_xaxes(categoryorder='category ascending', row=1, col=2)			
		fig.update_xaxes(categoryorder='category ascending', row=1, col=3)
		fig.update_xaxes(categoryorder='category ascending', row=1, col=4)
		fig.update_xaxes(categoryorder='category ascending', row=1, col=5)
		fig.update_layout(height=600, width=1500, showlegend=False) #, title_text="5-Porques (azul) vs MES (cinza)", showlegend=False
		st_grafico.write(fig)

		# Etapa de integração com o MES
		st.subheader('Integração com MES')
		st.write('Dados do MES no sistema')
		mes = load_mes()
		st.write(mes)

		# Upload do arquivo
		uploaded_file = st.file_uploader("Selecione o arquivo Excel para upload")
		if uploaded_file is not None:
			up_mes = upload_mes(uploaded_file, tipos)
			if up_mes is not None:
				st.write('A seguir os dados a serem armazenados no banco. Falta de dados significa que os dados do arquivo estão no sistema')
				st.write(up_mes)			
			
	if func_escolhida == 'Suporte Engenharia':
		st.subheader('Suporte da aplicação 5-Porques')
		
		# Campo para preenchimento de mensagem
		mensagem_suporte = st.text_input('Preencha o campo abaixo para reportar erros ou sugerir melhorias')
		
		# Campo para email
		email_contato = st.text_input('E-mail para contato (Opcional)')
		
		# Montagem da mensagem
		mensagem = mensagem_suporte + '\n\n' + email_contato
		
		# Envio da mensagem
		enviar_suporte = st.button('Enviar e-mail para suporte')
		if enviar_suporte:
			if mensagem_suporte != '':
				send_email('BRMAI0513@ambev.com.br', 4, '', mensagem, 0)
			else:
				st.error('Preencher a mensagem')
		
		# Video
		st.subheader('Tutoriais')
		video_file = open('streamlit-5pq-2021-06-05-09-06-63.webm', 'rb')
		video_bytes = video_file.read()
		st.video(video_bytes)
		
		st.subheader('Gestão da aplicação')
		
		# Reseta cache para refazer leitura dos bancos
		reset_db = st.button('Atualizar base de dados')
		if reset_db:
			caching.clear_cache()
		

	if func_escolhida == 'Gerenciamento das ações':
		st.subheader('Gerenciamento das ações geradas nos 5-Porques')
		
		fb_acao = read_acao()
				
		# Filtro para datas
		st.text('Filtro das ações')
		col1_ac, col2_ac, col3_ac, col4_ac = st.beta_columns(4)
		#col3_ac, col4_ac = st.beta_columns(2)

		#inicio_filtro_ac = col1_ac.date_input("Início") data = data[data['Definição do Evento'].isin(tipos)]
		#fim_filtro_ac = col2_ac.date_input("Fim")
		#filtrado_ac = (fn_acao[(fn_acao['Prazo'] >= inicio_filtro_ac) & (fn_acao['Prazo'] <= fim_filtro_ac)]) 
		filtrado_ac = fb_acao.copy()
		
		# Gera lista das linhas
		list_linhas_ac = list(linhas)
		list_linhas_ac.append('todos') 
		linhas_ac = col1_ac.selectbox("Selecione a linha", list_linhas_ac, list_linhas_ac.index('todos'))
		
		equipamentos_ac = ''
		lista_equipamentos_ac = []
		# Inicia o filtro com todas as linhas
		if linhas_ac == 'todos':
			equipamentos_ac = 'todos'
			lista_equipamentos_ac = ['todos']
			pass
		elif linhas_ac is not None and (str(linhas_ac) != 'nan'):
			filtrado_ac = filtrado_ac[filtrado_ac['Numero do 5-Porques'].str.contains(linhas_ac)]
			lista_equipamentos_ac = list(sap_nv3[sap_nv3['Linha'] == linhas_ac]['equipamento'])
			lista_equipamentos_ac.append('todos') 
		
		equipamentos_ac = col2_ac.selectbox("Selecione o equipamento", lista_equipamentos_ac, lista_equipamentos_ac.index('todos'))
		
		# Inicia o filtro com todos
		if equipamentos_ac == 'todos':
			pass
		elif equipamentos_ac is not None and (str(equipamentos_ac) != 'nan'):
			equipamentos_ac = equipamentos_ac.replace(" ", "")
			filtrado_ac = filtrado_ac[filtrado_ac['Numero do 5-Porques'].str.contains(equipamentos_ac)]

		# Gera lista dos donos
		list_dono_ac = list(filtrado_ac['Dono'].drop_duplicates())
		list_dono_ac.append('todos') 
		dono_ac = col3_ac.selectbox("Selecione o dono", list_dono_ac, list_dono_ac.index('todos'))
		
		# Inicia o filtro com todos
		if dono_ac == 'todos':
			pass
		elif dono_ac is not None and (str(dono_ac) != 'nan'):
			filtrado_ac = filtrado_ac[filtrado_ac['Dono'] == dono_ac]

		# Gera lista dos gestor	
		list_gestor_ac = list(filtrado_ac['Gestor'].drop_duplicates())
		list_gestor_ac.append('todos')  
		gestor_ac = col4_ac.selectbox("Selecione o gestor", list_gestor_ac, list_gestor_ac.index('todos'))
		
		# Inicia o filtro com todos
		if gestor_ac == 'todos':
			pass
		elif gestor_ac is not None and (str(gestor_ac) != 'nan'):
			filtrado_ac = filtrado_ac[filtrado_ac['Gestor'] == gestor_ac]	
		
		st.subheader('Ações atrasadas')	
		df_atrasadas = filtrado_ac[filtrado_ac['Status'] == 'Atrasada']
		for index, row in df_atrasadas.iterrows():
			text = str(row['Ação']) + ' ' + ' (Prazo: ' + str(row['Prazo']) + ')'
			with st.beta_expander(text):
				dados, botoes = st.beta_columns([7.5,2.5])
				dados.table(row[['Ação', 'Dono', 'Prazo', 'Gestor', 'E-mail', 'Numero do 5-Porques']])

				codigo_user = botoes.selectbox('Digite seu ID para alterar Ação ' + str(index), usuarios_fb['Codigo'])
				finalizar_acao = botoes.button('Finalizar Ação ' + str(index))
				descartar_acao = botoes.button('Cancelar Ação ' + str(index))
				novo_dono = botoes.selectbox('Novo dono da Ação ' + str(index), usuarios_fb['Nome'], usuarios_fb['Nome'].to_list().index(row['Dono']))
				alterar_dono = botoes.button('Alterar dono ' + str(index))
				
				if finalizar_acao:
					nome_editor = usuarios_fb.loc[usuarios_fb['Codigo'] == codigo_user, 'Nome']
					row['Editor'] = nome_editor.to_list()[0]
					row['Data'] = str(date.today())
					row['Status'] = 'Concluída'
					gravar_acao_edit(row)
					
					# Envia email para o dono da acao informando que a mesma esta concluida
					remetente = usuarios_fb.loc[usuarios_fb['Nome'] == row['Gestor'], 'Email'].to_list()
					send_email(remetente[0], 7, row['Editor'], row['Ação'], 0)
					
				if descartar_acao:
					nome_editor = usuarios_fb.loc[usuarios_fb['Codigo'] == codigo_user, 'Nome']
					row['Editor'] = nome_editor.to_list()[0]
					row['Data'] = str(date.today())
					row['Status'] = 'Cancelada'
					gravar_acao_edit(row)
					
					# Envia email para o dono da acao informando que a mesma esta concluida
					remetente = usuarios_fb.loc[usuarios_fb['Nome'] == row['Gestor'], 'Email'].to_list()
					send_email(remetente[0], 8, row['Editor'], row['Ação'], 0)
					
				if alterar_dono:

					row['Dono'] = novo_dono
					gravar_acao_edit(row)
					
					# Envia email para o dono da acao informando que a mesma foi criada 
					remetente = usuarios_fb.loc[usuarios_fb['Nome'] == row['Dono'], 'Email'].to_list()
					send_email(remetente[0], 6, row['Prazo'], row['Ação'], 0)
				
		st.subheader('Ações em aberto')	
		df_aberto = filtrado_ac[filtrado_ac['Status'] == 'Em aberto']
		for index, row in df_aberto.iterrows():
			text = str(row['Ação']) + '     ' + ' (Prazo: ' + str(row['Prazo']) + ')'
			with st.beta_expander(text):
				dados, botoes = st.beta_columns([7.5,2.5])
				dados.table(row[['Ação', 'Dono', 'Prazo', 'Gestor', 'E-mail', 'Numero do 5-Porques']])
				
				codigo_user = botoes.selectbox('Digite seu ID para alterar Ação ' + str(index), usuarios_fb['Codigo'])
				finalizar_acao = botoes.button('Finalizar Ação ' + str(index))
				descartar_acao = botoes.button('Cancelar Ação ' + str(index))
				novo_dono = botoes.selectbox('Novo dono da Ação ' + str(index), usuarios_fb['Nome'], usuarios_fb['Nome'].to_list().index(row['Dono']))
				alterar_dono = botoes.button('Alterar dono ' + str(index))
				
				if finalizar_acao:
					nome_editor = usuarios_fb.loc[usuarios_fb['Codigo'] == codigo_user, 'Nome']
					row['Editor'] = nome_editor.to_list()[0]
					row['Data'] = str(date.today())
					row['Status'] = 'Concluída'
					gravar_acao_edit(row)
					
					# Envia email para o dono da acao informando que a mesma esta concluida
					remetente = usuarios_fb.loc[usuarios_fb['Nome'] == row['Gestor'], 'Email'].to_list()
					send_email(remetente[0], 7, row['Editor'], row['Ação'], 0)

				if descartar_acao:
					nome_editor = usuarios_fb.loc[usuarios_fb['Codigo'] == codigo_user, 'Nome']
					row['Editor'] = nome_editor.to_list()[0]
					row['Data'] = str(date.today())
					row['Status'] = 'Cancelada'
					gravar_acao_edit(row)
					
					# Envia email para o dono da acao informando que a mesma foi criada
					remetente = usuarios_fb.loc[usuarios_fb['Nome'] == row['Gestor'], 'Email'].to_list()
					send_email(remetente[0], 8, row['Editor'], row['Ação'], 0)
					
				if alterar_dono:

					row['Dono'] = novo_dono
					gravar_acao_edit(row)
					
					# Envia email para o dono da acao informando que a mesma esta cancelada
					remetente = usuarios_fb.loc[usuarios_fb['Nome'] == row['Dono'], 'Email'].to_list()
					send_email(remetente[0], 6, row['Prazo'], row['Ação'], 0)							
						
		st.subheader('Ações concluídas')	
		df_concluidas = filtrado_ac[filtrado_ac['Status'] == 'Concluída']
		for index, row in df_concluidas.iterrows():
			text = str(row['Ação']) + '     ' + ' (Prazo: ' + str(row['Prazo']) + ')'
			with st.beta_expander(text):
				dados, botoes = st.beta_columns([7.5,2.5])
				dados.table(row[['Ação', 'Dono', 'Prazo', 'Gestor', 'E-mail', 'Numero do 5-Porques', 'Editor', 'Data']])
				
				reabrir_acao = botoes.button('Reabrir Ação ' + str(index))

				if reabrir_acao:
					row['Status'] = 'Em aberto'
					gravar_acao_edit(row)
		
		st.subheader('Ações canceladas')	
		df_canceladas = filtrado_ac[filtrado_ac['Status'] == 'Cancelada']
		for index, row in df_canceladas.iterrows():
			text = str(row['Ação']) + '     ' + ' (Prazo: ' + str(row['Prazo']) + ')'
			with st.beta_expander(text):
				dados, botoes = st.beta_columns([7.5,2.5])
				dados.table(row[['Ação', 'Dono', 'Prazo', 'Gestor', 'E-mail', 'Numero do 5-Porques', 'Editor', 'Data']])

				reabrir_acao = botoes.button('Reabrir Ação ' + str(index))

				if reabrir_acao:
					row['Status'] = 'Em aberto'
					gravar_acao_edit(row)
				
		# leitura da data atual
		data_atual = date.today()
		
		# zera flag para escrita no banco de dados (commit)
		flag = False
		
		# Faz uma copia do dataframe das acoes
		fb_acao_2 = fb_acao.copy()
		
		# Cria objeto batch para multiplas escritas
		batch = db.batch()
		
		# Itera sobre as acoes
		for index, row in fb_acao_2.iterrows():
			
			# Verifica se ha acoes atrasadas
			if (data_atual > row['Prazo']) & (row['Status'] == 'Em aberto'):
				
				# define documento e colecao
				chave = str(row['Numero do 5-Porques']) + '_' + str(row['Numero da ação'])
				ref = db.collection('acoes').document(chave)
				
				# altera os dados
				row['E-mail'] = 'Enviado'
				row['Status'] = 'Atrasada'
				row_string = row.astype(str)
				
				# Seta os valores para futuro commit
				batch.set(ref, row_string.to_dict())
				
				# seta flag para commit
				flag = True
				
				# Envia email para o dono da acao informando que a mesma esta atrasada
				remetente = usuarios_fb.loc[usuarios_fb['Nome'] == row['Dono'], 'Email'].to_list()
				send_email(remetente[0], 5, row['Numero do 5-Porques'], row['Ação'], 0)
				time.sleep(1)
			
			# verifica se ha acoes que estao como atrasadas mas ainda possuem prazo valido
			if (data_atual <= row['Prazo']) & (row['Status'] == 'Atrasada'):
				
				# define documento e colecao
				chave = str(row['Numero do 5-Porques']) + '_' + str(row['Numero da ação'])
				ref = db.collection('acoes').document(chave)
				
				# altera os dados
				row['Status'] = 'Em aberto'
				row_string = row.astype(str)
				
				# Seta os valores para futuro commit
				batch.set(ref, row_string.to_dict())
				
				# seta flag para commit
				flag = True
		
		# Verifica flag do commit 
		if flag == True:
			
			# escreve os dados no banco
			batch.commit()
			
			# limpa cache
			caching.clear_cache()
			
