# certif/

Deposer ici le bundle CA de l'entreprise, sous le nom `cacert.pem` : le meme
fichier que `python/certif/cacert.pem`.

Il est lu via `S3_CA_BUNDLE=certif/cacert.pem` dans le `.env` (chemin relatif a
la racine de `yb07/`). Un bundle CA ne contient que des certificats publics :
il n'est pas secret.
