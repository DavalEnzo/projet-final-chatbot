import numpy as np
import pandas as pd
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st 
import matplotlib.pyplot as plt
from collections import Counter


dfComment = pd.read_csv("./../train.csv")
counts = { 'toxic':dfComment[dfComment['toxic']==1]['toxic'].value_counts()[1],
           'severe_toxic':dfComment[dfComment['severe_toxic']==1]['severe_toxic'].value_counts()[1],
          'obscene':dfComment[dfComment['obscene']==1]['obscene'].value_counts()[1],
          'threat':dfComment[dfComment['threat']==1]['threat'].value_counts()[1],
          'insult':dfComment[dfComment['insult']==1]['insult'].value_counts()[1],
          'identity_hate':dfComment[dfComment['identity_hate']==1]['identity_hate'].value_counts()[1]
          }
dfCounts = pd.DataFrame(list(counts.items()))
dfCounts.rename(columns={0:'natureOfcomment',1:'Frequence'}, inplace=True)

st.markdown("<h1 style='text-align: center;'>Analyse de la donnée</h1>", unsafe_allow_html=True)



@st.cache_data
def showfig1():
    title = "RÉPARTION PAR CLASSE "
    fig1 = px.scatter(dfCounts, x='natureOfcomment', y='Frequence', color='natureOfcomment', size='Frequence', width=900, height=600, title=title)
    fig1.update_layout(title_x=0)  # Centre le titre horizontalement
    return fig1

# Appel de la fonction
fig = showfig1()
st.plotly_chart(fig, use_container_width = True)

"""
    On observe que la majorité des commentaires négatifs sont toxiques,
    la classe severe toxique est prohiminante suivi de la classe obscene et insult .
    On peut déja constater des différences signifcatives d'importances entre les différentes classes
    ### Le code du graphique
        @st.cache_data
        def showfig1():
            title = "RÉPARTION PAR CLASSE "
            fig1 = px.scatter(dfCounts, x='natureOfcomment', y='Frequence', color='natureOfcomment', size='Frequence', width=900, height=600, title=title)
            fig1.update_layout(title_x=0.3)  # Centre le titre horizontalement
            return fig1
"""

#deuxième graphe
#obtenir la classe des commentaires 

def natureOfcomment(x):
    listTypeComment = []
    valBin=x
    if valBin[0] == '1':
        listTypeComment.append('toxic')
    if valBin[1] == '1':
        listTypeComment.append('obscene')
    if valBin[2] == '1':
        listTypeComment.append('insult')
    if valBin[3] == '1':
        listTypeComment.append('identity_hate')
    if valBin[0] == '0' and valBin[1] == '0' and valBin[2] == '0' and valBin[3] == '0':
        listTypeComment.append('neutre')   
    return ", ".join(listTypeComment)

#convertir la valeur en decimal 

def valToDecimal(x):
    return int(x,2)

dfComment['valOfmultiClass'] = dfComment[['toxic','obscene','insult','identity_hate']].astype(str).apply(lambda x: ''.join(x), axis=1)
dfComment['natureOfmultiClass'] = dfComment['valOfmultiClass'].apply(natureOfcomment)
dfComment['decimalOfmultiClass'] = dfComment['valOfmultiClass'].apply(valToDecimal)
dfComment = dfComment.drop(dfComment[dfComment['decimalOfmultiClass'] == 0][:140000].index)

dfMulticlass = dfComment[['valOfmultiClass','natureOfmultiClass','decimalOfmultiClass']]

df1 = pd.DataFrame(dfComment['natureOfmultiClass'].value_counts()).reset_index()
df1.rename(columns={'index':'natureOfmultiClass','natureOfmultiClass':'counts'},inplace=True)
df2 = pd.DataFrame(dfComment['decimalOfmultiClass'].value_counts()).reset_index()
df2.rename(columns={'index':'decimalOfmultiClass','decimalOfmultiClass':'del'},inplace=True)

dfMulticlassClean = pd.concat([df1,df2],axis=1)
dfMulticlassClean.drop(['del'],axis=1,inplace=True)




@st.cache_data
def showfig2():
    color_scale = 'Viridis'
    fig2 = px.imshow(dfComment[['toxic','severe_toxic','obscene','threat','insult','identity_hate' ]].corr(numeric_only=True),text_auto = True,width=900,height=600,color_continuous_scale=color_scale,title="MATRICE DE CORRELATION")
    fig2.update_layout(title_x=0)
    return fig2

#la matrice de correlation
st.plotly_chart(showfig2(), use_container_width = True)

"""
     L'intérêt de ce graphique est de montrer qu'il y a énormément de commentaires 
     qui correspondent à différentes catégories, cependant pour un soucis de clarté,
     il convenait de réduire le plus possible le nombre de catégorie différente.
     Nous sommes passé de 41 sous catégories à 15 en enlevant seulement deux catégories: threat et severe_toxic

     d'implémenter une ia capable de modérer les comentaires!
    ### Le code du graphique?
        @st.cache_data
        def showfig2():
            color_scale = 'Viridis'
            fig2 = px.imshow(dfComment.corr(numeric_only=True),text_auto = True,width=900,height=600,color_continuous_scale=color_scale,title="MATRICE DE CORRELATION")
            fig2.update_layout(title_x=0)
            return fig2

        #la matrice de correlation
        st.plotly_chart(showfig2(), use_container_width = True)
"""



@st.cache_data
def showfig3():
    fig3 = px.bar(dfMulticlassClean,x=dfMulticlassClean['decimalOfmultiClass'],y=dfMulticlassClean['counts'],color=dfMulticlassClean['natureOfmultiClass'],width=900,height=600,title="LA FREQUENCE PAR MULTICLASS")
    x_tickvals = dfMulticlassClean['decimalOfmultiClass']
    fig3.update_xaxes(tickmode='array', tickvals=x_tickvals)
    fig3.update_layout(title_x=0)
    return fig3
#la frequence des multiclasses
st.plotly_chart(showfig3(), use_container_width = True)


"""
     L'intérêt de ce graphique est de montrer qu'il y a énormément de commentaire 
     qui correspondent à différente catégorie, cependant pour un soucis de clarté,
     il convenait de réduire le plus possible le nombre de catégorie différente.
     Nous sommes passé de 41 sous catégorie à 15 en enlevant seulement deux catégories: threat et severe_toxic

     d'implémenter une ia capable de modérer les comentaires!
    ### Le code du graphique?
        @st.cache_data
        def showfig3():
            fig3 = px.bar(dfMulticlassClean,x=dfMulticlassClean['decimalOfmultiClass'],y=dfMulticlassClean['counts'],color=dfMulticlassClean['natureOfmultiClass'],width=900,height=600,title="LA FREQUENCE PAR MULTICLASS")
            x_tickvals = dfMulticlassClean['decimalOfmultiClass']
            fig3.update_xaxes(tickmode='array', tickvals=x_tickvals)
            fig3.update_layout(title_x=0)
            return fig3
        #la frequence des multiclasses
        st.plotly_chart(showfig3(), use_container_width = True)
"""


# Chargez le fichier HTML
with open("/Users/kenuhnrimbert/Documents/ipssi_1er_anne/ml_project_1/front/pages/wordcloud_identity_hate.html", "r",encoding="UTF-8") as f:
    html_content = f.read()
    # Utilisez st.components.html pour afficher le contenu HTML
    st.components.v1.html(html_content, height=600, width=800)
    
with open("/Users/kenuhnrimbert/Documents/ipssi_1er_anne/ml_project_1/front/pages/wordcloud_insult.html", "r",encoding="UTF-8") as f:
    html_content = f.read()
    # Utilisez st.components.html pour afficher le contenu HTML
    st.components.v1.html(html_content, height=600, width=800)

with open("/Users/kenuhnrimbert/Documents/ipssi_1er_anne/ml_project_1/front/pages/wordcloud_obscene.html", "r",encoding="UTF-8") as f:
    html_content = f.read()
    # Utilisez st.components.html pour afficher le contenu HTML
    st.components.v1.html(html_content, height=600, width=800)

with open("/Users/kenuhnrimbert/Documents/ipssi_1er_anne/ml_project_1/front/pages/wordcloud_toxic.html", "r",encoding="UTF-8") as f:
    html_content = f.read()
    # Utilisez st.components.html pour afficher le contenu HTML
    st.components.v1.html(html_content, height=600, width=800)
    
# Exemple de données
data = {
    'Modèle': ['Model Complexe', 'Model Binaire'],
    'Pourcentage': [61, 90]
}

# Création d'un DataFrame à partir des données
df = pd.DataFrame(data)

# Création du graphique à barres horizontal
color_palette = ['#1f77b4', '#ff7f0e']

# Création du graphique à barres avec une palette de couleurs personnalisée
fig = px.bar(df, x='Pourcentage', y='Modèle', orientation='h', text='Pourcentage', 

color=data['Modèle']
             )

# Personnalisation du graphique
fig.update_layout(
    title='Comparaison des modèles',
    xaxis_title='Pourcentage',
    yaxis_title='Modèle',
    showlegend=False,
   
)

# Affichage du graphique
st.plotly_chart(fig, use_container_width=True)

"""
     Nous avons testé deux modeles pour tester l'efficité de notre,
     nettoyage de donnée et forcé de constater que plus il y a de catégories,
     moins le graphique est performant. D'ailleurs la quantité démesuré
     de 0 a longtemps faussé notre analyse sur l'accuracy du model

     d'implémenter une ia capable de modérer les comentaires!
    ### Le code du graphique?
        fig = px.bar(df, x='Pourcentage', y='Modèle', orientation='h', text='Pourcentage')

        # Personnalisation du graphique
        fig.update_layout(
            title='Comparaison des modèles',
            xaxis_title='Pourcentage',
            yaxis_title='Modèle',
            showlegend=False
        )

        # Affichage du graphique
        st.plotly_chart(fig, use_container_width=True)
"""



with open('/Users/kenuhnrimbert/Documents/ipssi_1er_anne/ml_project_1/front/pages/matrixMod1.html', 'r', encoding='utf-8') as file:
    plotly_html = file.read()

# Affichage du graphique Plotly en tant que composant HTML
st.components.v1.html(plotly_html, width=900, height=600)


with open('/Users/kenuhnrimbert/Documents/ipssi_1er_anne/ml_project_1/front/pages/matrixMod2.html', 'r', encoding='utf-8') as file:
    plotly_html = file.read()

# Affichage du graphique Plotly en tant que composant HTML
st.components.v1.html(plotly_html, width=900, height=600)









