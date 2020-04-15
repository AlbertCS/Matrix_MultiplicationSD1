import pywren_ibm_cloud as pywren
import random
import numpy as np
import ibm_boto3
import ibm_botocore
import pickle
import time

# Definició dimensions matriu
x0= 5; y0= 5
x1= y0; y1= 5

# Dimensio de la submatriu
a= 2

# Creació de les matrius a multiplicar
mat0 = np.random.randint(0, 10, (x0, y0))
mat1 = np.random.randint(0, 10, (x1, y1))

# Funció que divideix les matrius en submatrius més petites i les guarda al IBM_COS
def guardarMatriu(bucket_name, key, ibm_cos):
    print('I am processing the object cos://{}/{}'.format(bucket_name, key))
    iterdata = []
    i= 0; final= i+a; fila = []; iniciA = i
    # Bucle per recorrer les files de la matriuA
    while(i<final) & (i<mat0.shape[0]):
        # Guardo files
        fila.append(mat0[i])
        i=i+1
        # Nomes entro quan totes les files de la submatriu A estan guardades i començo a tractar les columnes de B
        if(i==final)| (i == mat0.shape[0]):
            j = 0; final = a; columna = []; iniciB = j
            # Bucle per recorrer les columnes de la matriuB
            while((j < final) & (j < mat1.shape[1])):
                # Guardo columnes
                columna.append([row[j] for row in mat1])
                j=j+1
                # Nomes entro quan totes les columnes de la submatriu B estan guardades
                if(j==final) | (j == mat1.shape[1]):
                    # Guardo les files de la submatriu A i les columnes de la submatriu B
                    array = [fila, columna]
                    # Serialitzo les submatrius
                    serialized = pickle.dumps(array, protocol=0)
                    # Penjo les submatrius a multiplicar a un fitxer al IBM_COS
                    ibm_cos.put_object(Bucket=bucket_name, Key=key + str(iniciA) + str(iniciB) + '.txt', Body=serialized)
                    # Guardo en iterdata el nom del fitxer que consistira en la posicio inicial en la submatriu A i B
                    iterdata.append(key + str(iniciA) + str(iniciB) + '.txt')
                    columna = []
                    # Quan acabo la primera submatriu de columnes augmento el rang per fer la seguent
                    final=j+a
                    iniciB = j
            fila = []
            # Quan acabo la primera submatriu de files augmento el rang per fer la seguent
            final=i+a
            iniciA = i
    return iterdata

# Funció que permet descarregar una matriu de IBM_COS, servia per comprovar si la funció guardar funcionava correctament
def descargarMatriu(bucket_name, key, ibm_cos):
    mat0 = np.empty((4, 4))
    mat0 = ibm_cos.get_object(Bucket=bucket_name, Key=key)['Body'].read()
    deserialized_mat0 = pickle.loads(mat0)
    return deserialized_mat0

# Funció que serà cridada per el map, es descarrega les submatrius del IBM_COS i les multiplica
def multiplicacio(nom_fitxer, ibm_cos):
    # Descarrreguem les submatrius de IBM_COS
    posicio_serialized = ibm_cos.get_object(Bucket='phyto.sd', Key=nom_fitxer)['Body'].read()
    # Deserialitzem les submatrius
    posicio = pickle.loads(posicio_serialized)
    resu = 0
    # Obtenim les files de A
    files  = np.asarray(posicio[0])
    # Obtenim les columnes de B
    columnes = np.asarray(posicio[1])
    # Transposem les columnes 
    columnes1 = columnes.transpose() 
    # Multipliquem les submatrius
    resu = np.dot(files, columnes1)
    return resu

# Funcio que ordena els resultats obtinguts del mat, copia les diferents submatrius en la matrius resultat
def ordenar(results):
    # Inicialitzacio de les matrius
    matriuFinal= np.zeros((x0,y1)).astype('int'); matriuTemp= np.empty((a,y1))
    matriuResu1 = np.asarray(results)
    count=0
    A = np.asarray(matriuResu1[count])
    i=0
    # Mentre encara tinguem matrius
    while(count <= matriuResu1.shape[0]):
        # Mirem si encara queden columnes en submatriu, si en queden anem afegint
        if(A.shape[1]!=y1):
            count= count + 1
            B = np.asarray(matriuResu1[count])
            A = np.append(A, B, axis=1)
        # Quan tenim totes les columnes de la submatriu tocara copiar tota la fila de submatrius a la matriu final
        if(A.shape[1] == y1 ):
            shA = np.shape(A)
            y=shA[0]+i
            # Comprovem que ens trobem dins del rang
            if(y<=matriuFinal.shape[0]):
                # Copiem a la matriu final la fila de submatrius
                matriuFinal[i:y,0:shA[1]]=A
            count= count + 1
            # Si ja hem copiat la fila a la matriu resultant i encara queden matrius, llegim la seguent
            if(count < matriuResu1.shape[0]):
                A = np.asarray(matriuResu1[count])
            i=i+shA[0]
    return matriuFinal

if __name__ == '__main__':
    
    # Comencem el rellotge
    start_time=time.time()
    # Creem els executors del cloud
    pw = pywren.ibm_cf_executor()
    # Cridem la funcio per guardar les submatrius al IBM_COS
    pw.call_async(guardarMatriu, ['phyto.sd', 'posicio'])
    iterdata = pw.get_result()

    # Map i reduce
    pw = pywren.ibm_cf_executor()
    matriuResu = pw.map_reduce(multiplicacio, iterdata, ordenar)
    pw.wait(matriuResu)
    # Calculem la diferencia de temps
    elapsed_time = time.time() - start_time
    print("Workers:")
    print(len(iterdata))
    print("Matriu0:")
    print(mat0)
    print("Matriu1:")
    print(mat1)
    print("MatriuResu:")
    print (pw.get_result())
    print("Temps d'execucio: ")
    print(elapsed_time)

    

   