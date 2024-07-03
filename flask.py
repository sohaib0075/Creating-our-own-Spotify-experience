import os
from flask import Flask, render_template, send_file

app = Flask(_name_)

# Function to read MP3 files from sugg.txt
def read_mp3_files(filename):
    with open(filename, 'r') as file:
        mp3_files = [line.strip() for line in file.readlines()]
    return mp3_files

# Function to find the path of the MP3 file
def find_file_path(root_folder, file_name):
    for foldername, subfolders, filenames in os.walk(root_folder):
        if file_name in filenames:
            return os.path.join(foldername, file_name)
    return None

# Route to render index.html with list of MP3 files
@app.route('/')
def index():
    mp3_files = read_mp3_files('sugg.txt')
    return render_template('index.html', mp3_files=mp3_files)

# Route to play MP3 file
@app.route('/play/<filename>')
def play(filename):
    root_folder = "fma_large"  # Specify the root folder to search in
    file_path = find_file_path(root_folder, filename)
    if file_path:
        return send_file(file_path)
    else:
        return "File not found", 404

if _name_ == '_main_':
    app.run(debug=True)