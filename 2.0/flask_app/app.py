from flask_app import app

if __name__ == '__main__':
    '''
    This script mainly exists for testing/launching without gunicorn.
    '''
    app.run(host='0.0.0.0', port=8000)
