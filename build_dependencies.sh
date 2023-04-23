#!/usr/bin/env bash

# Verificar se o pipenv está instalado
if [ -x "$(which pipenv)" ]
then
    # Verificar se o Pipfile.lock existe no diretório raiz
    if [ ! -e Pipfile.lock ]
    then
        echo 'ERROR - não foi possível encontrar Pipfile.lock'
        exit 1
    fi

    # Uso do Pipenv para criar o arquivo requirement.txt
    echo '... creating requirements.txt from Pipfile.lock'
    pipenv lock -r > requirements.txt

    # instalar pacotes em um diretório temporário e compactá-lo em um .zip
    touch requirements.txt  # salvaguarda no caso de não haver pacotes
    pip3 install -r requirements.txt --target ./packages

    # check to see if there are any external dependencies
    # if not then create an empty file to seed zip with
    if [ -z "$(ls -A packages)" ]
    then
        touch packages/empty.txt
    fi

    # zip dependencies
    if [ ! -d packages ]
    then 
        echo 'ERROR - pip failed to import dependencies'
        exit 1
    fi

    cd packages
    zip -9mrv packages.zip .
    mv packages.zip ..
    cd ..

    # Remover diretório temporário e requirements.txt
    rm -rf packages
    rm requirements.txt
    
    # adicionar módulos locais
    echo '... adding all modules from local utils package'
    zip -ru9 packages.zip dependencies -x dependencies/__pycache__/\*

    exit 0
else
    echo 'ERROR - pipenv is not installed --> run `pip3 install pipenv` to load pipenv into global site packages or install via a system package manager.'
    exit 1
fi