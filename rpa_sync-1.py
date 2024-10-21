import psycopg2
import requests
from datetime import date

# Conexão com os bancos de dados
def conectar_banco(host, database, user, password, port):
    """
    Função para estabelecer a conexão com um banco de dados PostgreSQL.
    Retorna a conexão e o cursor associados.
    """
    try:
        conexao = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cursor = conexao.cursor()
        return conexao, cursor
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados {database}: {e}")
        return None, None

# Consultas de APIs
def obter_senha_do_servico_externo(email):
    """
    Função para buscar a senha do usuário no serviço externo.
    """
    url = "http://127.0.0.1:5000/getPassword"
    try:
        response = requests.post(url, json={"email": email})
        response.raise_for_status()
        data = response.json()
        return data.get("password", "")
    except Exception as e:
        print(f"Erro ao obter a senha para o email {email}: {e}")
        return ""

def obter_foto_perfil_do_servico_externo(email):
    """
    Função para buscar a foto de perfil do usuário no serviço externo.
    """
    url = "http://127.0.0.1:5000/getProfilePhoto"
    try:
        response = requests.post(url, json={"email": email})
        response.raise_for_status()
        data = response.json()
        return data.get("url", "")
    except Exception as e:
        print(f"Erro ao obter a foto de perfil para o email {email}: {e}")
        return ""

# Sincronizações unidirecionais
def sincronizar_unidirecional_plano(cursor_db1, cursor_db2):
    """
    Sincronização unidirecional da tabela 'Plano' do Banco 1 para o Banco 2.
    """
    cursor_db1.execute("SELECT uId, cNome, cTipoPlano, nValor, cDescricao FROM Plano;")
    plano_records_db1 = cursor_db1.fetchall()

    for plano_record in plano_records_db1:
        plano_id, plano_nome, plano_tipo, plano_valor, plano_descricao = plano_record

        cursor_db2.execute("SELECT id FROM plano WHERE id = %s;", (plano_id,))
        plano_record_db2 = cursor_db2.fetchone()

        if plano_record_db2:
            cursor_db2.execute("""
                UPDATE plano
                SET nome = %s, tipo_usuario = %s, descricao = %s, valor = %s
                WHERE id = %s;
            """, (plano_nome, plano_tipo, plano_descricao, plano_valor, plano_id))
            print(f"Registro do plano com UUID {plano_id} atualizado no Banco 2.")
        else:
            cursor_db2.execute("""
                INSERT INTO plano (id, nome, tipo_usuario, descricao, valor)
                VALUES (%s, %s, %s, %s, %s);
            """, (plano_id, plano_nome, plano_tipo, plano_descricao, plano_valor))
            print(f"Novo registro do plano com UUID {plano_id} inserido no Banco 2.")

def sincronizar_unidirecional_boost(cursor_db1, cursor_db2):
    """
    Sincronização unidirecional da tabela 'Boost' do Banco 1 para o Banco 2.
    """
    cursor_db1.execute("SELECT uId, cNome, nValor, cDescricao, nPctBoost FROM Boost;")
    boost_records_db1 = cursor_db1.fetchall()

    for boost_record in boost_records_db1:
        boost_id, boost_nome, boost_valor, boost_descricao, boost_porcentagem = boost_record

        cursor_db2.execute("SELECT id FROM boost WHERE id = %s;", (boost_id,))
        boost_record_db2 = cursor_db2.fetchone()

        if boost_record_db2:
            cursor_db2.execute("""
                UPDATE boost
                SET nome = %s, descricao = %s, porcentagem = %s, valor = %s
                WHERE id = %s;
            """, (boost_nome, boost_descricao, boost_porcentagem, boost_valor, boost_id))
            print(f"Registro do boost com UUID {boost_id} atualizado no Banco 2.")
        else:
            cursor_db2.execute("""
                INSERT INTO boost (id, nome, descricao, porcentagem, valor)
                VALUES (%s, %s, %s, %s, %s);
            """, (boost_id, boost_nome, boost_descricao, boost_porcentagem, boost_valor))
            print(f"Novo registro do boost com UUID {boost_id} inserido no Banco 2.")

def sincronizar_unidirecional_plano_vantagens(cursor_db1, cursor_db2):
    """
    Sincronização unidirecional da tabela 'Plano_vantagem' do Banco 1 para o Banco 2.
    """
    cursor_db1.execute("SELECT uId, cVantagem, cAtivo, uId_Plano FROM Plano_vantagem;")
    vantagem_records_db1 = cursor_db1.fetchall()

    for vantagem_record in vantagem_records_db1:
        vantagem_id, vantagem_nome, vantagem_ativo, plano_id_db1 = vantagem_record
        vantagem_ativo_bool = True if vantagem_ativo == '1' else False

        cursor_db2.execute("SELECT id FROM plano_vantagens WHERE id = %s;", (vantagem_id,))
        vantagem_record_db2 = cursor_db2.fetchone()

        if vantagem_record_db2:
            cursor_db2.execute("""
                UPDATE plano_vantagens
                SET vantagem = %s, ativo = %s, plano_id = %s
                WHERE id = %s;
            """, (vantagem_nome, vantagem_ativo_bool, plano_id_db1, vantagem_id))
            print(f"Registro da vantagem com UUID {vantagem_id} atualizado no Banco 2.")
        else:
            cursor_db2.execute("""
                INSERT INTO plano_vantagens (id, vantagem, ativo, plano_id)
                VALUES (%s, %s, %s, %s);
            """, (vantagem_id, vantagem_nome, vantagem_ativo_bool, plano_id_db1))
            print(f"Novo registro da vantagem com UUID {vantagem_id} inserido no Banco 2.")

# Sincronizações bidirecionais
def sincronizar_bidirecional(cursor_db1, cursor_db2, tabela, campos_db1, campos_db2):
    """
    Função genérica para realizar a sincronização bidirecional de uma tabela entre Banco 1 e Banco 2.
    """
    # Extraindo dados do Banco 1
    query_db1 = f"SELECT {', '.join(campos_db1)} FROM {tabela};"
    cursor_db1.execute(query_db1)
    registros_db1 = cursor_db1.fetchall()

    # Extraindo dados do Banco 2
    query_db2 = f"SELECT {', '.join(campos_db2)} FROM {tabela};"
    cursor_db2.execute(query_db2)
    registros_db2 = cursor_db2.fetchall()

    registros_db1_dict = {registro[0]: registro for registro in registros_db1}
    registros_db2_dict = {registro[0]: registro for registro in registros_db2}

    # Inserindo novos registros do Banco 2 no Banco 1
    for uuid_db2, registro_db2 in registros_db2_dict.items():
        if uuid_db2 not in registros_db1_dict:
            email = registro_db2[1]  # Segundo campo é o email
            senha = obter_senha_do_servico_externo(email)
            foto_perfil = obter_foto_perfil_do_servico_externo(email)

            # Adiciona a senha aos campos para inserir no Banco 1
            registro_com_senha = list(registro_db2) + [senha, foto_perfil]

            campos_str_db1 = ', '.join(campos_db1)
            valores_str_db1 = ', '.join(['%s'] * len(campos_db1))
            insert_query_db1 = f"INSERT INTO {tabela} ({campos_str_db1}) VALUES ({valores_str_db1});"
            cursor_db1.execute(insert_query_db1, registro_com_senha)
            print(f"Novo registro de {tabela} com UUID {uuid_db2} inserido no Banco 1.")

    # Atualizando registros existentes do Banco 1 no Banco 2
    for uuid_db1, registro_db1 in registros_db1_dict.items():
        if uuid_db1 in registros_db2_dict:
            # Log para verificar UUID e dados de registros
            print(f"Atualizando registro com UUID {uuid_db1} no Banco 2.")

            # Mapear os valores dos campos do Banco 1 para os campos do Banco 2
            registro_db2_valores = []
            for campo in campos_db2[1:]:  # Começamos do índice 1 para pular o campo 'id'
                if campo in campos_db1:
                    registro_db2_valores.append(registro_db1[campos_db1.index(campo)])
                else:
                    # Adiciona um valor vazio ou nulo se o campo não existir em campos_db1
                    registro_db2_valores.append(None)

            # Log para verificar os valores que estamos tentando atualizar
            print(f"Valores para atualização: {registro_db2_valores}")

            # Criar a cláusula de atualização somente com os campos do Banco 2
            set_clause = ', '.join([f"{campo} = %s" for campo in campos_db2[1:]])
            print(f"Set clause para a atualização: {set_clause}")

            update_query_db2 = f"UPDATE {tabela} SET {set_clause} WHERE id = %s;"

            # Log completo da query antes de executá-la
            print(f"Query de atualização: {update_query_db2}")
            print(f"Valores usados na query: {registro_db2_valores + [uuid_db1]}")

            try:
                # Executar a atualização no Banco 2 usando apenas os valores dos campos correspondentes
                cursor_db2.execute(update_query_db2, registro_db2_valores + [uuid_db1])
                print(f"Registro de {tabela} com UUID {uuid_db1} atualizado no Banco 2.")
            except Exception as error:
                print(f"Erro ao atualizar registro com UUID {uuid_db1} no Banco 2: {error}")

def sincronizar_bidirecional_universitario(cursor_db1, cursor_db2):
    """
    Sincronização bidirecional da tabela 'Universitario' entre o Banco 1 e Banco 2.
    """
    campos_universitario_db1 = ['uId', 'cEmail', 'cNome', 'cDne', 'cDt_nascimento', 'cGenero', 'cPrefixo', 'cTelefone', 'cMunicipio', 'cDescricao', 'cSenha', 'cFotoPerfil']
    campos_universitario_db2 = ['id', 'email', 'nome', 'dne', 'dt_nascimento', 'genero', 'prefixo', 'telefone', 'municipio', 'bio']
    
    sincronizar_bidirecional(cursor_db1, cursor_db2, 'universitario', campos_universitario_db1, campos_universitario_db2)

def sincronizar_bidirecional_anunciante(cursor_db1, cursor_db2):
    """
    Sincronização bidirecional da tabela 'Anunciante' entre o Banco 1 e Banco 2.
    """
    campos_anunciante_db1 = ['uId', 'cEmail', 'cNome', 'cDt_nascimento', 'cGenero', 'cMunicipio', 'cPrefixo' 'cTelefone', 'cDescricao', 'cSenha', 'cFotoPerfil']
    campos_anunciante_db2 = ['id', 'email', 'nome', 'dt_nascimento', 'genero', 'municipio', 'prefixo', 'telefone', 'bio']
    sincronizar_bidirecional(cursor_db1, cursor_db2, 'anunciante', campos_anunciante_db1, campos_anunciante_db2)

def main():
    # Conectando aos bancos de dados
    connection_db1, cursor_db1 = conectar_banco("localhost", "hestia_1ano", "postgres", "5511", "5432")
    connection_db2, cursor_db2 = conectar_banco("localhost", "hestia_2ano", "postgres", "5511", "5432")

    if cursor_db1 and cursor_db2:
        try:
            # Sincronizações unidirecionais (por exemplo: plano, boost, plano_vantagens)
            sincronizar_unidirecional_plano(cursor_db1, cursor_db2)
            sincronizar_unidirecional_boost(cursor_db1, cursor_db2)
            sincronizar_unidirecional_plano_vantagens(cursor_db1, cursor_db2)

            # Sincronizações bidirecionais para universitario e anunciante
            sincronizar_bidirecional_universitario(cursor_db1, cursor_db2)
            sincronizar_bidirecional_anunciante(cursor_db1, cursor_db2)

            # Commit das alterações
            connection_db1.commit()
            connection_db2.commit()
            print("Sincronizações finalizadas com sucesso.")

        except Exception as error:
            print("Erro ao realizar as sincronizações:", error)
        finally:
            # Fechando cursores e conexões
            cursor_db1.close()
            connection_db1.close()
            cursor_db2.close()
            connection_db2.close()
    else:
        print("Falha na conexão com os bancos de dados.")

if __name__ == "__main__":
    main()