import psutil
import requests
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_batch, DictCursor
from constantes2 import HEADERS, DB_CONFIG, CODIGO_PARA_UNIDADE, COORDINATION_IDS, TABELA_ALUNOS_GERAL, ANO_LETIVO_ATUAL

# Função para criar tabelas no banco de dados (se não existirem)
def criar_tabelas():
    try:
        with psycopg2.connect(**DB_CONFIG) as conexao:
            with conexao.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS alunos_lize_teste (
                    id TEXT PRIMARY KEY,
                    nome TEXT,
                    matricula TEXT UNIQUE,
                    email TEXT,
                    classes TEXT[],
                    ativo BOOLEAN DEFAULT TRUE,
                    ano_letivo INTEGER,
                    PRIMARY KEY (matricula, ano_letivo) -- Permite o mesmo aluno em anos diferentes
                );
                """)
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS turmas_lize_teste (
                    id TEXT PRIMARY KEY,
                    nome TEXT,
                    coordination TEXT,
                    school_year INTEGER
                );
                """)
                conexao.commit()
                print("✅ Tabelas criadas ou verificadas com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")

# Função para obter dados da API com paginação
def obter_dados_api(url, tipo_dado):
    dados = []
    pagina = 1
    while url:
        print(f"🔄 Obtendo {tipo_dado} da página {pagina}...")  # Indicador de progresso
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            dados.extend(data.get("results", []))
            url = data.get("next")
            pagina += 1
        else:
            print(f"❌ Erro ao obter {tipo_dado} da API: {response.status_code}")
            break
    print(f"✅ Total de {tipo_dado} coletados: {len(dados)}")
    return dados

def persistir_dados_em_lote(tabela, dados, colunas, conflito):
    if not dados:
        return
    
    try:
        # Configuração inicial do batch_size com fallback
        try:
            batch_size = 1000 if psutil.virtual_memory().percent < 70 else 500
        except Exception:
            batch_size = 500  # Fallback se psutil falhar
        
        print(f"🔧 Configuração inicial - Batch size: {batch_size} | RAM usada: {psutil.virtual_memory().percent}%")

        with psycopg2.connect(**DB_CONFIG) as conexao:
            with conexao.cursor() as cursor:
                placeholders = ", ".join(["%s"] * len(colunas))
                update_set = ", ".join(f"{coluna} = EXCLUDED.{coluna}" 
                                     for coluna in colunas if coluna != conflito)
                
                query = f"""
                    INSERT INTO {tabela} ({", ".join(colunas)})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflito}) DO UPDATE
                    SET {update_set};
                """
                
                for i in range(0, len(dados), batch_size):
                    # Reavalia o batch_size a cada 5 lotes (opcional)
                    if i > 0 and i % (5 * batch_size) == 0:
                        try:
                            batch_size = 1000 if psutil.virtual_memory().percent < 70 else 500
                            print(f"🔧 Ajuste dinâmico - Novo batch size: {batch_size} | RAM: {psutil.virtual_memory().percent}%")
                        except Exception:
                            pass  # Mantém o batch_size anterior
                    
                    batch = dados[i:i + batch_size]
                    valores = [(
                        dado.get("id"),
                        dado.get("name"),
                        dado.get("enrollment_number"),
                        dado.get("email"),
                        [str(c["id"]) for c in dado.get("classes", [])],
                        dado.get("is_active", True),
                        ANO_LETIVO_ATUAL
                    ) if tabela == "alunos_lize_teste" else (
                        dado.get("id"),
                        dado.get("name"),
                        dado.get("coordination"),
                        dado.get("school_year")
                    ) for dado in batch]

                    execute_batch(cursor, query, valores)
                    conexao.commit()
                    print(f"✅ Lote {i//batch_size + 1} persistido - {len(batch)} registros | RAM: {psutil.virtual_memory().percent}%")
                
                print(f"✅ Todos os dados persistidos na tabela {tabela}")
    except Exception as e:
        print(f"❌ Erro ao persistir dados em lote: {e}")
        if 'conexao' in locals():  # Garante que a conexão seja fechada em caso de erro
            conexao.rollback()

# Função para obter alunos do banco de dados local
def obter_alunos_banco():
    """
    Obtém alunos do banco local em streaming (sem carregar tudo na RAM).
    Retorna um gerador que produz um aluno por vez.
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conexao:
            # Cursor server-side (withhold=True) + itersize para controle de memória
            with conexao.cursor(name='alunos_stream', withhold=True) as cursor:
                cursor.itersize = 500  # Buffer interno de 500 registros
                cursor.execute(f"""
                    SELECT unidade, sit, matricula, nome, turma
                    FROM {TABELA_ALUNOS_GERAL}
                    WHERE turma::NUMERIC >= 11500::NUMERIC
                    ORDER BY matricula;
                """)
             
                for aluno in cursor:
                    # Remove espaços e retorna tupla (mais eficiente que lista)
                    yield (
                        aluno[0],                   # unidade_cod
                        aluno[1],                   # sit
                        aluno[2].strip(),          # matricula
                        aluno[3].strip(),          # nome
                        aluno[4].strip()           # turma
                    )
    except Exception as e:  
        print(f"❌ Erro ao obter alunos do banco: {e}")
        yield from ()  # Retorna gerador vazio em caso de erro

def obter_id_aluno_por_matricula(matricula):
    try:
        with psycopg2.connect(**DB_CONFIG) as conexao:
            with conexao.cursor() as cursor:
                cursor.execute("SELECT id FROM alunos_lize_teste WHERE matricula = %s;", (matricula,))
                return cursor.fetchone()[0] if cursor.rowcount > 0 else None
    except Exception as e:
        print(f"❌ Erro ao buscar ID do aluno: {e}")
        return None

def aluno_tem_turma(aluno_id, turma_id):
    try:
        with psycopg2.connect(**DB_CONFIG) as conexao:
            with conexao.cursor() as cursor:
                cursor.execute("""
                    SELECT classes FROM alunos_lize_teste 
                    WHERE id = %s;
                """, (aluno_id,))
                result = cursor.fetchone()
                return result and turma_id in result[0]
    except Exception as e:
        print(f"❌ Erro ao verificar turma do aluno: {e}")
        return False

# Função para definir a etapa de ensino com base no código da turma
def definir_etapa_ensino(codigo_turma):
    if len(codigo_turma) < 3:
        return None
    terceiro_digito = codigo_turma[2]
    if codigo_turma.startswith("11"):
        return "Anos Iniciais" if terceiro_digito == "5" else "Anos Finais"
    elif codigo_turma.startswith("2"):
        return "Ensino Médio"
    return None

def processar_alunos():
    # 1. Fase de coleta de dados
    print("⏳ Iniciando coleta de dados...")
    
    # 1.1. Coletar dados da API
    alunos_api = obter_dados_api("https://staging.lizeedu.com.br/api/v2/students/", "alunos")
    turmas_api = obter_dados_api(f"https://staging.lizeedu.com.br/api/v2/classes/?school_year={datetime.now().year}", "turmas")
    
    # 1.2. Persistência otimizada em lote
    print("⏳ Persistindo dados da API...")
    persistir_dados_em_lote("alunos_lize_teste", alunos_api, 
                          ["id", "nome", "matricula", "email", "classes", "ativo"], "matricula")
    persistir_dados_em_lote("turmas_lize_teste", turmas_api, 
                          ["id", "nome", "coordination", "school_year"], "id")
    
    # 2. Fase de preparação
    print("⏳ Preparando ambiente para processamento...")
    
    # 2.1. Obter alunos do banco local com tratamento de duplicados
    alunos_banco = obter_alunos_banco()
    if not alunos_banco:
        print("❌ Nenhum aluno encontrado no banco local.")
        return
    
    # Pré-filtro de duplicados
    matriculas_unicas = set()
    alunos_para_processar = []
    
    for aluno in alunos_banco:
        matricula = aluno[2]
        if matricula not in matriculas_unicas:
            matriculas_unicas.add(matricula)
            alunos_para_processar.append(aluno)
        
    # 2.2. Carregar caches otimizados
    with psycopg2.connect(**DB_CONFIG) as conexao:
        with conexao.cursor() as cursor:
            # Cache de alunos
            cursor.execute("""
                SELECT matricula, id, nome, email, ativo, classes 
                FROM alunos_lize_teste;
            """)
            alunos_cache = {
                row[0]: {
                    'id': row[1],
                    'nome': row[2],
                    'email': row[3],
                    'ativo': row[4],
                    'classes': row[5] or []
                } for row in cursor.fetchall()
            }
            
            # Cache de turmas
            cursor.execute("""
                SELECT nome, coordination, id 
                FROM turmas_lize_teste 
                WHERE school_year = %s;
            """, (datetime.now().year,))
            turmas_cache = {}
            for nome, coordination, id_turma in cursor.fetchall():
                turmas_cache.setdefault((coordination, nome), []).append(id_turma)
    
    # 3. Fase de processamento principal
    print(f"⏳ Processando {len(alunos_para_processar)} alunos...")
    contador = 0
    tempo_inicio = datetime.now()
    matriculas_processadas = set()  # Novo: rastreia matrículas já processadas

    for unidade_cod, sit, matricula, nome, turma in alunos_para_processar:
        # Verificação e correção de duplicatas
        if matricula in matriculas_processadas:
            aluno_info = alunos_cache.get(matricula)
            if aluno_info and aluno_info['ativo']:
                if desativar_aluno(aluno_info['id'], nome, matricula):
                    alunos_cache[matricula]['ativo'] = False
            continue  # Pula para o próximo aluno

        matriculas_processadas.add(matricula)  # Registra matrícula
        contador += 1
        turma = turma.strip()
        
        # 3.1. Validações iniciais
        unidade_nome = CODIGO_PARA_UNIDADE.get(unidade_cod)
        if not unidade_nome:
            continue
            
        etapa_ensino = definir_etapa_ensino(turma)
        if not etapa_ensino:
            continue
            
        coordination_id = COORDINATION_IDS.get(unidade_nome, {}).get(etapa_ensino)
        if not coordination_id:
            continue
        
        # 3.2. Gerenciamento de status
        aluno_info = alunos_cache.get(matricula)
        situacao_aluno = int(sit)
        
        if situacao_aluno in [2, 4]:  # Aluno deve estar INATIVO
            if aluno_info and aluno_info['ativo']:
                if desativar_aluno(aluno_info['id'], nome, matricula):
                    alunos_cache[matricula]['ativo'] = False
            continue
        else:  # Aluno deve estar ATIVO
            if aluno_info and not aluno_info['ativo']:
                if ativar_aluno(aluno_info['id'], nome, matricula):
                    alunos_cache[matricula]['ativo'] = True
        
        # 3.3. Inserção/Atualização do aluno
        email_gerado = f"{matricula}@alunos.smrede.com.br"
        if not aluno_info:
            if inserir_aluno(nome, matricula, email_gerado):
                aluno_id = obter_id_aluno_por_matricula(matricula)
                if aluno_id:
                    alunos_cache[matricula] = {
                        'id': aluno_id,
                        'nome': nome,
                        'email': email_gerado,
                        'ativo': True,
                        'classes': []
                    }
        else:
            if aluno_info['nome'] != nome or aluno_info['email'] != email_gerado:
                if atualizar_aluno(aluno_info['id'], nome, matricula, email_gerado):
                    alunos_cache[matricula].update({
                        'nome': nome,
                        'email': email_gerado
                    })
        
        # 3.4. Associação à turma
        if matricula in alunos_cache and alunos_cache[matricula]['ativo']:
            aluno_id = alunos_cache[matricula]['id']
            turma_key = (coordination_id, turma)
            
            if turma_key in turmas_cache:
                for turma_id in turmas_cache[turma_key]:
                    if turma_id not in alunos_cache[matricula]['classes']:
                        if associar_aluno_turma(aluno_id, turma_id):
                            alunos_cache[matricula]['classes'].append(turma_id)
        
        # Log de progresso
        if contador % 1000 == 0:
            tempo_decorrido = (datetime.now() - tempo_inicio).total_seconds()
            velocidade = contador / tempo_decorrido if tempo_decorrido > 0 else 0
            print(f"↳ Progresso: {contador}/{len(alunos_para_processar)} | Velocidade: {velocidade:.2f} alunos/seg")
    
    # 4. Relatório final
    tempo_total = (datetime.now() - tempo_inicio).total_seconds()
    print(f"\n✅ Processamento concluído!\n"
          f"• Alunos processados: {contador}\n"
          f"• Tempo total: {int(tempo_total // 60)}m {int(tempo_total % 60)}s\n"
          f"• Velocidade média: {contador/max(1, tempo_total):.2f} alunos/seg")

def atualizar_status_aluno_local(id_aluno, status):
    try:
        with psycopg2.connect(**DB_CONFIG) as conexao:
            with conexao.cursor() as cursor:
                cursor.execute("""
                    UPDATE alunos_lize_teste 
                    SET ativo = %s 
                    WHERE id = %s;
                """, (status, id_aluno))
                conexao.commit()
                print(f"✅ Status do aluno (ID: {id_aluno}) atualizado para {'ativo' if status else 'inativo'}.")
    except Exception as e:
        print(f"❌ Erro ao atualizar status do aluno no banco local: {e}")

# Funções da API (mantidas conforme o original)
def associar_aluno_turma(student_id, school_class_id):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{student_id}/set_classes/"
    payload = {"school_classes": [str(school_class_id)]}
    response = requests.post(url, headers=HEADERS, json=payload)
    return response.status_code == 200

def atualizar_aluno(aluno_id, nome, matricula, email):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{aluno_id}/"
    data = {"name": nome, "enrollment_number": matricula, "email": email}
    response = requests.put(url, headers=HEADERS, json=data)
    return response.status_code == 200

def desativar_aluno(id_aluno, nome_aluno):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{id_aluno}/disable/"
    response = requests.post(url, headers=HEADERS, json={})
    if response.status_code in [200, 204]:
        print(f"❌ Aluno {nome_aluno} desativado com sucesso!")
        atualizar_status_aluno_local(id_aluno, False)  # Atualiza o banco local
        return True
    else:
        print(f"❌ Erro ao desativar aluno {nome_aluno}: {response.status_code}")
        return False

def ativar_aluno(id_aluno, nome_aluno):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{id_aluno}/enable/"
    response = requests.post(url, headers=HEADERS, json={})
    if response.status_code in [200, 204]:
        print(f"✅ Aluno {nome_aluno} ativado com sucesso!")
        atualizar_status_aluno_local(id_aluno, True)  # Atualiza o banco local
        return True
    else:
        print(f"❌ Erro ao ativar aluno {nome_aluno}: {response.status_code}")
        return False

def inserir_aluno(nome, matricula, email):
    url = "https://staging.lizeedu.com.br/api/v2/students/"
    data = {"name": nome, "enrollment_number": matricula, "email": email}
    response = requests.post(url, headers=HEADERS, json=data)
    return response.status_code == 201

# Execução principal
if __name__ == "__main__":
    criar_tabelas()
    processar_alunos()