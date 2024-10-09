from flask import Flask, request, jsonify
import joblib
app = Flask(__name__)

fModel2 = './mon_modele_test2.pkl'
fModel1 = './mon_modele.pkl'


isModel1 = True

""" model = joblib.load(fModel1) if isModel1 else joblib.load(fModel2) 
vecteur = './mon_vecteur.pkl' if isModel1 else './mon_vecteur_test2.pkl' """
model1 = joblib.load(fModel1)
model2 = joblib.load(fModel2)
fVecteur1 = './mon_vecteur.pkl'
fVecteur2 = './mon_vecteur_test2.pkl'

def decodeurBinaire(valeurBinaire) :
    arrType = [
    "toxic",
    "severe_toxic",
    "obscene",
    "threat",
    "insult",
    "identity,_hate"
]

    typologie = 'faites attention au propos que vous tenez, car ils sont'
    if valeurBinaire != '0000':
        for i in range (len(valeurBinaire)) :
            print(int(valeurBinaire[i]) == 1)
            if int(valeurBinaire[i]) == 1 :
                typologie += ' ' +  arrType[i]
    else :
        typologie = 'votre texte est conforme à ce que nous demandons'
    return typologie

with open(fVecteur1, 'rb') as f:
    vecteurizer1 = joblib.load(f)

with open(fVecteur2, 'rb') as f:
    vecteurizer2 = joblib.load(f)

joblib.dump(vecteurizer1, fVecteur1)

joblib.dump(vecteurizer2, fVecteur2)

# Route POST
@app.route('/api/data', methods=['POST'])
def post_data():
    data = request.get_json()
    nouvelle_phrase = data['message']
    vecteur_entree1 = vecteurizer1.transform([nouvelle_phrase])
    prediction1 = model1.predict(vecteur_entree1)

    vecteur_entree2 = vecteurizer2.transform([nouvelle_phrase])
    prediction2 = model2.predict(vecteur_entree2)

    print(prediction1, prediction2)
    value = 0
    if (prediction1[0] != 0 and prediction2[0] != 0) :
        valeurBinaire = str('{:06b}'.format(prediction1[0]))[2:]
        typologie = decodeurBinaire(valeurBinaire)
        value = "1"
    elif (prediction1[0] == 0 and prediction2[0] == 0) :
        typologie = "vos propos sont approprié"
        value = "0"
    elif (prediction1[0] != 0 and prediction2[0] == 0) :
        typologie = "vos propos sont approprié"
        value = "0"
    elif (prediction1[0] == 0 and prediction2[0] != 0):
        typologie = "vos propos sont approprié"
        value = "0"

    if data:
        return jsonify({'message':typologie, 'value': value})
    else:
        return jsonify({'message': 'Erreur : Aucune donnée POST n\'a été fournie.'}), 400


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5550, debug=True)