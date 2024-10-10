# Dockerfile pour le front-end

FROM python:3.8-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /frontend

# Copier les fichiers de l'application dans le conteneur
COPY . /frontend

# Installer les dépendances nécessaires
RUN pip install --upgrade pip && \
    pip install -r /frontend/requirements.txt

# Exposer le port utilisé par l'application (par exemple, Streamlit)
EXPOSE 8501
EXPOSE 8502

# Commande par défaut pour démarrer l'application
CMD ["streamlit", "run", "HOME.py", "--server.port=8501"]
