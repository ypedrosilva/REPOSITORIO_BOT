import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import Conflict
from urllib.parse import urlencode
import psycopg2
from psycopg2.extras import RealDictCursor
import sys

# Configuração de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configurações
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
AFFILIATE_BASE_URL = 'https://1wtjjp.com/?p=hgjy'
DATABASE_URL = os.getenv('DATABASE_URL', '')

def get_db_connection():
    """Conecta ao banco PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco: {e}")
        return None

def init_database():
    """Inicializa as tabelas se não existirem"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                fbclid TEXT,
                useragent TEXT,
                ip TEXT,
                fbb TEXT,
                sub1 TEXT,
                sub2 TEXT,
                sub3 TEXT,
                sub4 TEXT,
                sub5 TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        logger.info("Banco de dados inicializado")
    except Exception as e:
        logger.error(f"Erro ao inicializar banco: {e}")
    finally:
        conn.close()

def get_click_data(click_id):
    """Busca dados do clique no banco"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('''
            SELECT fbclid, useragent, ip, fbb, sub1, sub2, sub3, sub4, sub5
            FROM clicks 
            WHERE click_id = %s AND used = FALSE
        ''', (click_id,))
        row = cursor.fetchone()
        
        if row:
            logger.info(f"Dados encontrados para clique {click_id}")
            return dict(row)
    except Exception as e:
        logger.error(f"Erro ao buscar clique {click_id}: {e}")
    finally:
        conn.close()
    
    return None

def mark_click_as_used(click_id):
    """Marca o clique como usado"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute('UPDATE clicks SET used = TRUE WHERE click_id = %s', (click_id,))
        conn.commit()
        logger.info(f"Clique {click_id} marcado como usado")
    except Exception as e:
        logger.error(f"Erro ao marcar clique como usado: {e}")
    finally:
        conn.close()

def save_user_data(user_id, username, first_name, last_name, fbclid=None, useragent=None, ip=None, fbb=None, sub1=None, sub2=None, sub3=None, sub4=None, sub5=None):
    """Salva ou atualiza dados do usuário"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO users 
            (user_id, username, first_name, last_name, fbclid, useragent, ip, fbb, sub1, sub2, sub3, sub4, sub5)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) 
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                fbclid = COALESCE(EXCLUDED.fbclid, users.fbclid),
                useragent = COALESCE(EXCLUDED.useragent, users.useragent),
                ip = COALESCE(EXCLUDED.ip, users.ip),
                fbb = COALESCE(EXCLUDED.fbb, users.fbb),
                sub1 = COALESCE(EXCLUDED.sub1, users.sub1),
                sub2 = COALESCE(EXCLUDED.sub2, users.sub2),
                sub3 = COALESCE(EXCLUDED.sub3, users.sub3),
                sub4 = COALESCE(EXCLUDED.sub4, users.sub4),
                sub5 = COALESCE(EXCLUDED.sub5, users.sub5),
                updated_at = CURRENT_TIMESTAMP
        ''', (user_id, username, first_name, last_name, fbclid, useragent, ip, fbb, sub1, sub2, sub3, sub4, sub5))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar dados do usuário: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_user_data(user_id):
    """Obtém dados do usuário"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM users WHERE user_id = %s', (user_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
    except Exception as e:
        logger.error(f"Erro ao buscar dados do usuário: {e}")
    finally:
        conn.close()
    
    return None

def generate_affiliate_link(user_data):
    """Gera link de afiliado com parâmetros"""
    params = {}
    
    if user_data.get('sub1'):
        params['sub1'] = user_data['sub1']
    elif user_data.get('fbclid'):
        params['sub1'] = user_data['fbclid']
    
    if user_data.get('sub2'):
        params['sub2'] = user_data['sub2']
    elif user_data.get('useragent'):
        params['sub2'] = user_data['useragent']
    
    if user_data.get('sub3'):
        params['sub3'] = user_data['sub3']
    elif user_data.get('ip'):
        params['sub3'] = user_data['ip']
    
    if user_data.get('sub4'):
        params['sub4'] = user_data['sub4']
    elif user_data.get('fbb'):
        params['sub4'] = user_data['fbb']
    
    if user_data.get('sub5'):
        params['sub5'] = user_data['sub5']
    elif user_data.get('user_id'):
        params['sub5'] = str(user_data['user_id'])
    
    if params:
        query_string = urlencode(params)
        return f"{AFFILIATE_BASE_URL}&{query_string}"
    else:
        return AFFILIATE_BASE_URL

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para /start"""
    try:
        user = update.effective_user
        user_id = user.id
        
        logger.info(f"Comando /start recebido de user_id: {user_id}, args: {context.args}")
        
        click_data = None
        if context.args and len(context.args) > 0:
            click_id = context.args[0].strip().replace('-', '')
            
            if len(click_id) == 12 and click_id.isalnum():
                click_data = get_click_data(click_id)
                
                if click_data:
                    logger.info(f"✅ Clique {click_id} encontrado")
                    mark_click_as_used(click_id)
                else:
                    logger.warning(f"⚠️ Clique {click_id} não encontrado")
        
        # Salvar dados do usuário
        save_user_data(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            fbclid=click_data.get('fbclid') if click_data else None,
            useragent=click_data.get('useragent') if click_data else None,
            ip=click_data.get('ip') if click_data else None,
            fbb=click_data.get('fbb') if click_data else None,
            sub1=click_data.get('sub1') if click_data else None,
            sub2=click_data.get('sub2') if click_data else None,
            sub3=click_data.get('sub3') if click_data else None,
            sub4=click_data.get('sub4') if click_data else None,
            sub5=click_data.get('sub5') if click_data else None
        )
        
        user_data = get_user_data(user_id)
        
        if not user_data:
            await update.message.reply_text("❌ Erro ao salvar dados. Tente novamente.")
            return
        
        affiliate_link = generate_affiliate_link(user_data)
        
        welcome_message = f"""👋 Olá, {user.first_name}!

Seu link de afiliado foi gerado com sucesso!

🔗 {affiliate_link}

Use este link para acessar a oferta com todos os seus parâmetros de rastreamento."""
        
        await update.message.reply_text(welcome_message)
        logger.info(f"✅ Mensagem enviada para user_id: {user_id}")
        
    except Exception as e:
        logger.error(f"❌ Erro no handler /start: {e}", exc_info=True)
        try:
            await update.message.reply_text("❌ Ocorreu um erro. Por favor, tente novamente.")
        except:
            pass

async def link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para /link"""
    user = update.effective_user
    user_data = get_user_data(user.id)
    
    if not user_data:
        await update.message.reply_text("❌ Você precisa usar /start primeiro!")
        return
    
    affiliate_link = generate_affiliate_link(user_data)
    await update.message.reply_text(f"🔗 Seu link de afiliado:\n\n{affiliate_link}")

async def dados(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para /dados"""
    user = update.effective_user
    user_data = get_user_data(user.id)
    
    if not user_data:
        await update.message.reply_text("❌ Nenhum dado encontrado. Use /start primeiro!")
        return
    
    dados_text = f"""📊 Seus dados salvos:

🆔 User ID: {user_data['user_id']}
👤 Nome: {user_data.get('first_name', '')} {user_data.get('last_name', '') or ''}
📱 Username: @{user_data.get('username', 'N/A')}

📥 Dados do Facebook:
• FB Click ID: {user_data.get('fbclid', 'N/A')}
• User Agent: {str(user_data.get('useragent', 'N/A'))[:50]}...
• IP: {user_data.get('ip', 'N/A')}
• FBB: {user_data.get('fbb', 'N/A')}

🔢 Parâmetros:
• sub1: {user_data.get('sub1', 'N/A')}
• sub2: {user_data.get('sub2', 'N/A')}
• sub3: {user_data.get('sub3', 'N/A')}
• sub4: {user_data.get('sub4', 'N/A')}
• sub5: {user_data.get('sub5', 'N/A')}"""
    
    await update.message.reply_text(dados_text)

def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN não configurado!")
        return
    
    if not DATABASE_URL:
        logger.error("DATABASE_URL não configurado!")
        return
    
    init_database()
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("link", link))
    application.add_handler(CommandHandler("dados", dados))
    
    logger.info("Bot iniciado!")
    
    try:
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,  # Limpar updates pendentes ao iniciar
            close_loop=False
        )
    except Conflict as e:
        logger.error(f"❌ CONFLITO: Outra instância do bot está rodando!")
        logger.error(f"Erro: {e}")
        logger.error("Por favor, verifique se há outro bot rodando e pare-o.")
        logger.error("No Railway, verifique se há múltiplos serviços BOT rodando.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

