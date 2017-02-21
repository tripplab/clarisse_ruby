Breve descripción de los archivos

El archivo README.md  contiene una breve descripción de lo que hace el programa

El archivo clarisse contiene el código que fue programado usando ruby

El archivo plantilla contiene los parámetros que se utilizan en cada corrida de clarisse y éste archivo puede modificarse (debe). Este archivo sustituye al archivo control dentro de codeml.

El archivo config contiene las entradas para cada una de las ejecuciones del archivo plantilla. Este archivo también puede modificarse dependiendo del número de corridas del archivo plantilla.

Los archivos de entrada son el alineamiento de codones basado en proteínas que puede hacerse con RevTrans o alg2nal para que genere un archivo con extensión .phy el cual se caracteriza por tener el número de entradas y el tamaño de la secuencia más larga en el alineamiento.

El otro archivo de entrada es el árbol filogenético marcado con las ramas que se asume están bajo diferentes tasas de sustitución. El árbol tiene que estar en formato newick que tiene la extensión .nwk
