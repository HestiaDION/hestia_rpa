from flask import Flask, request, jsonify
from pymongo import MongoClient
from flasgger import Swagger
import logging
from os import getenv
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
Swagger(app)

# Configuração de logging
logging.basicConfig(
    filename='api-mongo_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuração de conexão com o MongoDB
client = MongoClient(getenv('URI_MONGODB'))
db = client[getenv('MONGO_DBNAME')]
collection = db[getenv('MONGO_COLLECTION')]

# Rota para expor a senha através do email
@app.route('/get-password', methods=['POST'])
def get_password():
    """
    Expor a senha do usio através do email
    ---
    tags:
      - User
    parameters:
      - name: email
        in: body
        type: string
        required: true
        description: O email do usio para obter a senha
        schema:
          type: object
          properties:
            email:
              type: string
              example: "jose@gmail.com"
    responses:
      200:
        description: Senha do usio
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
        description: Usio não encontrado
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Usio não encontrado"
    """
    data = request.get_json()
    if not data or 'email' not in data:
        logging.error("Email is missing in the request for /get-password")
        return jsonify({'error': 'Email é obrigatório'}), 400
    
    email = data['email']
    logging.info(f"Received request for /get-password with email: {email}")
    
    user = collection.find_one({'email': email})
    if not user:
        logging.error(f"No user found with email: {email}")
        return jsonify({'error': 'Usio não encontrado'}), 404
    
    logging.info(f"User found for email {email}")
    return jsonify({'senha': user['senha']})

# Rota para expor a foto através do email
@app.route('/get-photo', methods=['POST'])
def get_photo():
    """
    Expor a foto do usio através do email
    ---
    tags:
      - User
    parameters:
      - name: email
        in: body
        type: string
        required: true
        description: O email do usio para obter a foto
        schema:
          type: object
          properties:
            email:
              type: string
              example: "jose@gmail.com"
    responses:
      200:
        description: URL da foto do usio
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
        description: Usio não encontrado
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Usio não encontrado"
    """
    data = request.get_json()
    if not data or 'email' not in data:
        logging.error("Email is missing in the request for /get-photo")
        return jsonify({'error': 'Email é obrigatório'}), 400
    
    email = data['email']
    logging.info(f"Received request for /get-photo with email: {email}")
    
    user = collection.find_one({'email': email})
    if not user:
        logging.error(f"No user found with email: {email}")
        return jsonify({'error': 'Usio não encontrado'}), 404
    
    logging.info(f"User found for email {email}")
    return jsonify({'urlFoto': user['urlFoto']})

if __name__ == '__main__':
    logging.info("Starting Flask application")
    app.run(debug=True, use_reloader=False)
