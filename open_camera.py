from flask import Flask, render_template
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('open_camera.html')

if __name__ == '__main__':
    # Start the server on host 0.0.0.0 to make it accessible from any device on the same network
    app.run(host='0.0.0.0', port=5000)
