# Importações
import psycopg2
import requests
from os import getenv
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Função para conectar ao banco de dados
def conectar_banco(uri):
    try:
        conexao = psycopg2.connect(uri)
        cursor = conexao.cursor()
        return conexao, cursor
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None, None

# Função para obter a senha do usuário a partir do e-mail
def get_senha(email):
    url = "http://127.0.0.1:5000/get-password"
    try:
        response = requests.post(url, json={"email": email})
        response.raise_for_status()
        data = response.json()
        return data.get("password", "")
    except Exception as e:
        print(f"Erro ao obter a senha para o email {email}: {e}")
        return ""

# Função para obter a foto de perfil do usuário a partir do e-mail
def get_foto_perfil(email):
    url = "http://127.0.0.1:5000/get-photo"
    try:
        response = requests.post(url, json={"email": email})
        response.raise_for_status()
        data = response.json()
        return data.get("url", "")
    except Exception as e:
        print(f"Erro ao obter a foto de perfil para o email {email}: {e}")
        return ""

# Faz a sincronização da tabela plano
def sync_plano(cursor_db1, cursor_db2, connection_db2):
    try:
        # Seleção corrigida com o nome atualizado da coluna
        cursor_db1.execute("SELECT uId, cNome, cTipoUsuario, nValor, cDescricao FROM Plano;")
        plano_records_db1 = cursor_db1.fetchall()

        for plano_record in plano_records_db1:
            plano_id, plano_nome, plano_tipo_usuario, plano_valor, plano_descricao = plano_record

            cursor_db2.execute("SELECT id FROM plano WHERE id = %s;", (plano_id,))
            plano_record_db2 = cursor_db2.fetchone()

            if plano_record_db2:
                cursor_db2.execute("""
                    UPDATE plano
                    SET nome = %s, tipo_conta = %s, descricao = %s, valor = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                """, (plano_nome, plano_tipo_usuario, plano_descricao, plano_valor, plano_id))
                print(f"Registro do plano com UUID {plano_id} atualizado no Banco 2.")
            else:
                cursor_db2.execute("""
                    INSERT INTO plano (id, nome, tipo_conta, descricao, valor, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                """, (plano_id, plano_nome, plano_tipo_usuario, plano_descricao, plano_valor))
                print(f"Novo registro do plano com UUID {plano_id} inserido no Banco 2.")

        connection_db2.commit()
    except Exception as e:
        connection_db2.rollback()
        print(f"Erro ao sincronizar tabela Plano: {e}")

# Faz a sincronização da tabela plano_vantagens
def sync_plano_vantagens(cursor_db1, cursor_db2, connection_db2):
    try:
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

        connection_db2.commit()
    except Exception as e:
        connection_db2.rollback()
        print(f"Erro ao sincronizar tabela Plano_vantagens: {e}")

# Função para sincro nizar a tabela universitário entre os dois bancos de dados
def sync_universitario(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM Universitario;")
        universitario_records_db1 = cursor_db1.fetchall()

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * from universitario")
        universitario_records_db2 = cursor_db2.fetchall()

        ids_db1 = [x[0] for x in universitario_records_db1]

        # Inserindo as informações no banco do 1 ano
        for universitario in universitario_records_db2:
            # Desestruturando as informações do universitário
            (uid, email, nome, dne, dt_nascimento, genero, prefixo, telefone, municipio, universidade,
            plano_id, bio, tipo_conta, created_at, updated_at) = universitario
            
            # Caso o usuário já existir no banco de dados do 1 ano, ignora ele
            if uid in ids_db1:
                continue
            else:
                # Consulta os serviços externos para recuperar a senha e foto de perfil do universitário
                senha = get_senha(email)
                foto_perfil = get_foto_perfil(email)

                # Converte o plano para 0 ou 1
                if plano_id != None:
                    plano = '1'
                else:
                    plano = '0'

                # Insere no banco 1
                cursor_db1.execute("""INSERT INTO Universitario 
                                   (uId, cDne, cNome, cUsername, cEmail, cSenha, dDtNascimento, cGenero,
                                   cMunicipio, cPrefixo, cTel, cPlano, cFotoPerfil, cDescricao,
                                   cNmFaculdade)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", 
                                   (uid, dne, nome, nome, email, senha, dt_nascimento, genero, municipio, prefixo, telefone,
                                    plano, foto_perfil, bio, universidade))
                
                print(f'Novo registro de universitario com UUID {uid} inserido no Banco 1.')
        
        # Atualizando as informações do banco do 2 ano
        for universitario in universitario_records_db1:
            # Desestruturando as informações do universitário
            (uId, cDne, cNome, cUsername, cEmail, cSenha, dDtNascimento, cGenero, cMunicipio,
            cPrefixo, cTel, cPlano, cFotoPerfil, cDescricao, uId_Anuncio, cNmFaculdade) = universitario

            # Caso o plano seja '0' no banco 1, ele é inserido como 'None' (resulta em null)
            if cPlano == '0':
                plano = None
            else:
                # Caso seja '1' no banco 1, ele permanece o mesmo
                cursor_db2.execute("SELECT plano_id FROM universitario u WHERE u.id = %s", (uId,))
                result = cursor_db2.fetchone()
                plano = result[0] if result else None

            # Atualiza as informações do universitário
            cursor_db2.execute("""
                UPDATE universitario SET
                email = %s, nome = %s, dne = %s, dt_nascimento = %s, genero = %s, prefixo = %s,
                telefone = %s, municipio = %s, universidade = %s, plano_id = %s, bio = %s
                WHERE id = %s;
            """, (cEmail, cNome, cDne, dDtNascimento, cGenero, cPrefixo, cTel, cMunicipio, cNmFaculdade,
                  plano, cDescricao, uId))

            print(f'Registro de universitario com UUID {uid} atualizado no Banco 2.')

        # Confirma as alterações em ambos os bancos
        connection_db1.commit()
        connection_db2.commit()

    except Exception as e:
        connection_db1.rollback()
        connection_db2.rollback()
        print(f"Erro ao sincronizar tabela Universitario: {e}")

def sync_anunciante(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM Anunciante;")
        anunciante_records_db1 = cursor_db1.fetchall()

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * from anunciante")
        anunciante_records_db2 = cursor_db2.fetchall()

        ids_db1 = [x[0] for x in anunciante_records_db1]

        # Inserindo as informações no banco do 1 ano
        for anunciante in anunciante_records_db2:
            # Desestruturando as informações do universitário
            (uid, email, nome, dt_nascimento, genero, municipio, prefixo, telefone,
            bio, tipo_conta, plano_id, created_at, updated_at) = anunciante

            if uid in ids_db1:
                continue
            else:
                # Consulta os serviços externos para recuperar a senha e foto de perfil do anunciante
                senha = get_senha(email)
                foto_perfil = get_foto_perfil(email)

                if plano_id != None:
                    plano = '1'
                else:
                    plano = '0'

                cursor_db1.execute("""
                    INSERT INTO Anunciante 
                    (uId, cNome, cUsername, cEmail, cSenha, dDtNascimento, cGenero,
                    cMunicipio, cPrefixo, cTel, cPlano, cFotoPerfil, cDescricao)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (uid, nome, nome, email, senha, dt_nascimento, genero, municipio, prefixo, telefone,
                          plano, foto_perfil, bio))
                
                print(f'Novo registro de anunciante com UUID {uid} inserido no Banco 1.')
        
        # Atualizando as informações do banco do 2 ano
        for anunciante in anunciante_records_db1:
            (uId, cNome, cUsername, cEmail, cSenha, dDtNascimento, cGenero, cMunicipio,
            cPrefixo, cTel, cPlano, cDescricao, cFotoPerfil) = anunciante
            
            # Caso o plano seja '0' no banco 1, ele é inserido como 'None' (resulta em null)
            if cPlano == '0':
                plano = None
            else:
                # Caso seja '1' no banco 1, ele permanece o mesmo
                cursor_db2.execute("SELECT plano_id FROM anunciante u WHERE u.id = %s", (uId,))
                result = cursor_db2.fetchone()
                plano = result[0] if result else None

            # Atualiza as informações do anunciante
            cursor_db2.execute("""
                UPDATE anunciante SET
                email = %s, nome = %s, dt_nascimento = %s, genero = %s, prefixo = %s,
                telefone = %s, municipio = %s, plano_id = %s, bio = %s
                WHERE id = %s;
            """, (cEmail, cNome, dDtNascimento, cGenero, cPrefixo, cTel, cMunicipio,
                  plano, cDescricao, uId))

            print(f'Registro de anunciante com UUID {uid} atualizado no Banco 2.')

        # Confirma as alterações em ambos os bancos
        connection_db1.commit()
        connection_db2.commit()


    except Exception as e:
        connection_db1.rollback()
        connection_db2.rollback()
        print(f"Erro ao sincronizar tabela Anunciante: {e}")

def sync_pagamento(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM Pagamento;")
        pagamento_records_db1 = cursor_db1.fetchall()

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * from pagamento_plano;")
        pagamento_records_db2 = cursor_db2.fetchall()

        ids_db1 = [x[0] for x in pagamento_records_db1]
        # Inserindo as informações no banco do 1 ano
        for pagamento in pagamento_records_db2:
            # Desestruturando as informações do pagamento
            (uid, nome, email, plano_id, pago, created_at, updated_at) = pagamento
            if uid not in ids_db1:
                ativo = '0'

                info_user = cursor_db2.execute("SELECT * FROM get_user_uuid_by_email(%s);", (email,))
                info_user = cursor_db2.fetchone()
                (tipo_user, uid_user) = info_user

                dt_fim = datetime.now() + relativedelta(months=1)

                total = cursor_db2.execute("SELECT valor FROM plano where id = %s;", (plano_id,))
                total = cursor_db2.fetchone()[0]

                if tipo_user == 'anunciante':

                    cursor_db1.execute("""
                        INSERT INTO Pagamento 
                        (uId, cAtivo, dDtFim, nPctDesconto, nTotal, uId_Anunciante, uId_Plano,
                        uId_Universitario)
                        VALUES 
                            (%s, %s, %s, %s, %s, %s, %s, %s);
                        """, (uid, ativo, dt_fim, 0, total, uid_user, plano_id, None))
                else:
                    cursor_db1.execute("""
                        INSERT INTO Pagamento 
                        (uId, cAtivo, dDtFim, nPctDesconto, nTotal, uId_Anunciante, uId_Plano,
                        uId_Universitario)
                        VALUES 
                            (%s, %s, %s, %s, %s, %s, %s, %s);
                        """, (uid, ativo, dt_fim, 0, total, None, plano_id, uid_user))
                    
                print(f'Novo registro de pagamento com UUID {uid} inserido no Banco 1.')

        # Atualizando as informações do banco do 2 ano
        for pagamento in pagamento_records_db1:
            (uid, cAtivo, dDtFim, nPctDesconto, nTotal, uId_Anunciante, uId_Plano, uId_Universitario) = pagamento
            
            # Caso o cAtivo seja '0' no banco 1, ele é inserido como 'False'
            pago = cAtivo == '1'

            # Atualiza as informações do pagamento
            cursor_db2.execute("""
                UPDATE pagamento_plano SET
                pago = %s
                WHERE id = %s;
            """, (pago, uid))

            print(f'Registro de pagamento com UUID {uid} atualizado no Banco 2.')

        # Confirma as alterações em ambos os bancos
        connection_db1.commit()
        connection_db2.commit()


    except Exception as e:
        connection_db1.rollback()
        connection_db2.rollback()
        print(f"Erro ao sincronizar tabela Pagamento: {e}")

# Função principal
def main():
    load_dotenv()

    # Define as URIs de conexão
    db1_uri = getenv('URI_BANCO_1')
    db2_uri = getenv('URI_BANCO_2')

    # Utiliza a função conectar_banco() para gerar as conexões e cursores dos dois bancos
    connection_db1, cursor_db1 = conectar_banco(db1_uri)
    connection_db2, cursor_db2 = conectar_banco(db2_uri)

    # Caso não haja erros na conexão dos dois bancos de dados
    if cursor_db1 and cursor_db2:
        try:
            sync_plano(cursor_db1, cursor_db2, connection_db2)  # Sincroniza as tabelas plano
            sync_plano_vantagens(cursor_db1, cursor_db2, connection_db2)  # Sincroniza as tabelas plano_vantagens
            sync_universitario(cursor_db1, cursor_db2, connection_db1, connection_db2)  # Sincroniza as tabelas universitario
            sync_anunciante(cursor_db1, cursor_db2, connection_db1, connection_db2)  # Sincroniza as tabelas anunciante
            sync_pagamento(cursor_db1, cursor_db2, connection_db1, connection_db2)  # Sincroniza as tabelas pagamento

            print("Sincronização finalizada com sucesso.")
        except Exception as error:
            print("Erro ao realizar a sincronização:", error)
        finally:
            # Fecha as conexões
            cursor_db1.close()
            connection_db1.close()
            cursor_db2.close()
            connection_db2.close()
    else:
        print("Falha na conexão com os bancos de dados.")

if __name__ == "__main__":
    main()
