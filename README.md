# elasticsearch Client
Le projet consistant à indexer et rechercher dans le fonds Eur-Lex:

1. Indexer chaque notice d'Eur-Lex dans un index Elastic Search en prenant en compte toutes les balises XML concernant la version française.

2.  Rechercher par un ou plusieurs termes :

- La recherche ne doit pas être sensible à la casse ni aux accents.

- Dans le cas d'une recherche avec plusieurs termes, l'ordre des termes doit être pris en compte dans les résultats retournés.
Ex : Pour une recherche "Insertion pénitentiaire", ne pas ressortir les termes "pénitentiaire d'insertion"

- Il faut pouvoir cumuler les termes recherchés et prendre en compte les exclusions. 
Ex : bureau des politiques sociales + insertion pénitentiaire - administration

3. Rechercher par date de publication et par identifiant d'une notice Eur-lex

Echantillon : 
https://drive.google.com/a/splayce.com/file/d/0BwPZc0wJy15cWGhCLW1xTHVtZXM/view?usp=sharing
