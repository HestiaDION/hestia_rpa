from flask import Flask, request, jsonify
from pymongo import MongoClient
from flasgger import Swagger

app = Flask(__name__)
Swagger(app)

# Configuração de conexão com o MongoDB
client = MongoClient("mongodb+srv://aed_hestia:gYeeRwGKnAFge6tC@banco-hestia.tp5sk.mongodb.net/?retryWrites=true&w=majority")
db = client['db-hestia']  # Seleciona o banco de dados
collection = db['info_user']  # Seleciona a coleção 'info_user'

# Rota para expor a senha através do email
@app.route('/get-password', methods=['POST'])
def get_password():
    """
    Expor a senha do usuário através do email
    ---
    tags:
      - User
    parameters:
      - name: email
        in: body
        type: string
        required: true
        description: O email do usuário para obter a senha
        schema:
          type: object
          properties:
            email:
              type: string
              example: "jose@gmail.com"
    responses:
      200:
        description: Senha do usuário
        schema:
          type: object
          properties:
            senha:
              type: string
              example: "jakarta"
      400:
        description: Erro quando o email não é fornecido
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Email é obrigatório"
      404:
        description: Usuário não encontrado
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Usuário não encontrado"
    """
    # Obtém o parâmetro 'email' do corpo da requisição
    data = request.get_json()
    if not data or 'email' not in data:
        print("[ERROR] Email is missing in the request")
        return jsonify({'error': 'Email é obrigatório'}), 400
    
    email = data['email']
    print(f"[DEBUG] Received request for /get-password with email: {email}")
    
    # Procura o usuário na coleção pelo email
    user = collection.find_one({'email': email})
    if not user:
        print(f"[ERROR] No user found with email: {email}")
        return jsonify({'error': 'Usuário não encontrado'}), 404
    
    # Retorna a senha do usuário encontrado
    print(f"[DEBUG] User found: {user}")
    return jsonify({'senha': user['senha']})

# Rota para expor a foto através do email
@app.route('/get-photo', methods=['POST'])
def get_photo():
    """
    Expor a foto do usuário através do email
    ---
    tags:
      - User
    parameters:
      - name: email
        in: body
        type: string
        required: true
        description: O email do usuário para obter a foto
        schema:
          type: object
          properties:
            email:
              type: string
              example: "jose@gmail.com"
    responses:
      200:
        description: URL da foto do usuário
        schema:
          type: object
          properties:
            urlFoto:
              type: string
              example: "content://com.miui.gallery.open/raw/%2Fstorage%2Femulated%2F0%2FAndroid%2Fmedia%2Fcom.whatsapp%2FWhatsApp%2FMedia%2FWhatsApp%20Images%2FIMG-20241018-WA0020.jpg"
      400:
        description: Erro quando o email não é fornecido
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Email é obrigatório"
      404:
        description: Usuário não encontrado
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Usuário não encontrado"
    """
    # Obtém o parâmetro 'email' do corpo da requisição
    data = request.get_json()
    if not data or 'email' not in data:
        print("[ERROR] Email is missing in the request")
        return jsonify({'error': 'Email é obrigatório'}), 400
    
    email = data['email']
    print(f"[DEBUG] Received request for /get-photo with email: {email}")
    
    # Procura o usuário na coleção pelo email
    user = collection.find_one({'email': email})
    if not user:
        print(f"[ERROR] No user found with email: {email}")
        return jsonify({'error': 'Usuário não encontrado'}), 404
    
    # Retorna a URL da foto do usuário encontrado
    print(f"[DEBUG] User found: {user}")
    return jsonify({'urlFoto': user['urlFoto']})

if __name__ == '__main__':
    # Mensagem indicando que a aplicação está sendo iniciada
    print("[INFO] Starting Flask application")
    app.run(debug=True, use_reloader=False)
