const { MongoClient, ServerApiVersion } = require('mongodb');
const fs = require('fs');

// URL de conexión a la base de datos
const uri = 'mongodb+srv://caraven90:EqlgdNhlriHGrugS@cluster0.mg6hkhi.mongodb.net/?retryWrites=true&w=majority';

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: false,
    deprecationErrors: true,
  }
});

const dbName = 'ejercicio';

async function run() {
  try {
    // Connect the client to the server	(optional starting in v4.7)
    await client.connect();
    // Send a ping to confirm a successful connection
    await client.db("admin").command({ ping: 1 });
    console.log("Pinged your deployment. You successfully connected to MongoDB!");
    // const session = client.startSession();
    // session.startTransaction();
    //Base de datos
    const db = client.db(dbName);

    /*
    //Collection test
    const collection = db.collection('test');
    //Documento de prueba a insertar
    const document = { nombre: 'Carlos', apellido: 'Avendaño', edad: 33};
    //Insercion del documento a la coleccion
    const resultado = await collection.insertOne(document);
    console.log('Documento insertado', resultado.insertedId);
    */

    /*
    //Collection hd_logpresences
    await inserta_collection_json (db, 'hd_logpresences', 'hd_logpresences.json');
    */

    /*
    //Collection viewershipcontents
    await inserta_collection_json (db, 'viewershipcontents', 'viewershipcontents.json');
    */

    /*
    //Collection viewershippresences_example
    await inserta_collection_json (db, 'viewershippresences_example', 'viewershippresences.json');
    */

    const hd_logpresences = db.collection("hd_logpresences");
    const viewershipcontents = db.collection("viewershipcontents");
    const viewershippresences = db.collection('viewershippresences_example');
    
    //Se instancian la colección viewershipcontents
    const inst_viewershipcontents = await viewershipcontents.find({});
    var contents = [];
    
    await inst_viewershipcontents.forEach((viewershipcontent) => {
       contents.push(viewershipcontent.metadata.contents);
    });

/*
    //Análisis de datos de viewershipcontents
    const inst_viewershipcontents = await viewershipcontents.find({});

    try {
      await inst_viewershipcontents.forEach (async (viewershipcontent) => {
        const contents = viewershipcontent.metadata.contents;
        try {
          await contents.forEach (async (content) => {
            //console.log(content._id._id);
            const presences = content.presenceIds;
            const granular = content.granularPresenceIds;
            try
            {
              await presences.forEach ( async (presence) => {
                try {
                  const presence_hd_logpresences_id = await hd_logpresences.find({'_id._id': "presence._id"}).toArray();
                  const presence_hd_logpresences_panelistId = await hd_logpresences.find({'panelistId._id': "presence._id"}).toArray();
                  console.log('presenceId: ', presence_hd_logpresences_id);
                  console.log('panelistId: ',presence_hd_logpresences_panelistId);
                  // await session2.commitTransaction();
                  // console.log('Transacción completada');
                }
                catch (error)
                {
                  console.error('Error al buscar la presence id', error);
                }
                finally {
                  console.log("presenceId " + presence._id + " buscada con éxito");
                }
              });
            }
            catch (error)
            {
                console.error('Error al buscar la presenceId', error);
            }
            finally {
              console.log("Presencias de content" + content._id._id + " barridas con éxito");
            }
          });
        }
        catch (error) {
            console.error('Error al barrer los contenidos', error);
        }
        finally {
          console.log("Contenidos barridos con éxito");
        }
      });
    }
    catch (error) {
      console.error('Error al obtener los viewershipcontents', error);
    }
    finally
    {
      console.log("viewershipcontents obtenidos con exito");
    }
*/
    
//    //Prueba para obtener un registro de hd_logpresences por su id
//    const query = {'_id': {_id: "646d062a18c048b67b7a0ca4"}};
//    const sample_hd_logpresences = await hd_logpresences.findOne(query);
//    console.log('Muestra hd_logpresences:', sample_hd_logpresences);
    
    const sample_content = find_content_by_id(contents, '6467caf2818efc00963d5295');
    console.log('Muestra content:', sample_content);


    //Se analizan los diferentes panelistId que vienen en la colección hd_logpresences
    const panelists_hd_logpresences = await hd_logpresences.distinct("panelistId");
    
//    panelists_hd_logpresences.forEach(async (panelist) =>
//    {
//        try {
//            panelist.presences_raw = [];
//            panelist.presences = [];
//        
//            console.log('1. ', panelist._id);
//            const panelist_presences_by_date = hd_logpresences.find({'panelistId._id': panelist._id}).sort({timestamp: 1}); //Ordena por el campo "timestamp" en orden ascendente
//
//            console.log('2. ',panelist._id);
//            //console.log(panelist_presences_by_date);
//            //Se inicializan las variables auxiliares para segmentar las presencias por tiempos
//            let previousRecord = null;
//            var batches = [];
//            let currentBatch = [];
//            const timeThreshold = 15000; //Tiempo que dura una presencia de hd_logpresences en ms
//            const tolerancia = 2500;
//            var timeDifference;
//            var timeAct, timPrev;
//            //Se barre la coleccion hd_logpresences ya ordenada por timestamp y por panelistId
//            var cont = 0, contLote = 0;
//            console.log('3.');
//                await panelist_presences_by_date.forEach((document,i) => {
//                    try {
//                        console.log(document._id);
//                        if(previousRecord) {
//                          //
//                          timeAct = Date.parse(document.timestamp._date);
//                          timePrev = Date.parse(previousRecord.timestamp._date);
//                          timeDifference = timeAct - timePrev;
//
//                          if (timeDifference > (timeThreshold - tolerancia) && timeDifference < (timeThreshold + tolerancia))
//                          {
//                            // console.log("Misma presencia: " + timeDifference);
//                          }
//                          else
//                          {
//                            console.log("Diferente presencia: " + timeDifference);
//                          }
//
//                          // if (timeDifference > timeThreshold)
//                          // {
//                          // console.log('Lote ' + contLote + ':', currentBatch);
//                          //   const panelistId = panelists_hd_logpresences.find((panelistId) => panelistId._id == document.panelistId._id);
//                          //   if (panelistId)
//                          //   {
//                          //     panelistId.presences_raw.push(currentBatch);
//                          //   }
//                          //   else {
//                          //     const none = panelists_hd_logpresences.find((panelistId) => panelistId._id == 'none');
//                          //     if (none) {
//                          //       none.presences_raw.push(currentBatch);
//                          //     }
//                          //   }
//                          //   //batches.push(currentBatch);
//                          //   currentBatch = [];
//                          //   contLote++;
//                          // }
//                        }
//
//                        // currentBatch.push(document);
//                        previousRecord = document;
//                    } catch (e) {
//                        console.error("Error 2", e);
//                    } finally {
//                      console.log("Terminado 2");
//                    }
//                });
//            }
//            catch (e) {
//                console.error('Error 1:', e);
//            } finally {
//                console.log('Terminado 1');
//            }
//
//    });

    //Se  muestra el último lote, si tiene datos
    // if(currentBatch.length > 0) {
    //   const panelistId = panelists_hd_logpresences.find((panelistId) => panelistId._id == previousRecord.panelistId._id);
    //   if (panelistId)
    //   {
    //     panelistId.presences_raw.push(currentBatch);
    //   }
    //   else {
    //     const none = panelists_hd_logpresences.find((panelistId) => panelistId._id == 'none');
    //     if (none) {
    //       none.presences_raw.push(currentBatch);
    //     }
    //   }
    //   contLote++;
    //   //console.log('Lote final:', currentBatch);
    // }

    // console.log('batches: ' +  contLote);
    // await panelists_hd_logpresences.forEach((panelistId) => {
    //   console.log(panelistId._id + ': ', panelistId.presences_raw.length);
    // });
  // await session.commitTransaction();
  // console.log('Transacción completada');
  
  /* FUncion para filtrar contenido
//    let result = filter_content_by_date(contents, "2023-05-19T18:48:49.353Z", "2023-05-19T19:38:03.438Z");
//    console.log(result);
//    console.log(result.length);
   */



    /*
    //Se analizan los diferentes tipos de presencias que vienen en la colección hd_logpresences
    const type_hd_logpresences = await hd_logpresences.distinct("type");
    console.log('presence_type in hd_logpresences: ',type_hd_logpresences);

    const inst_hd_logpresences = await hd_logpresences.find().toArray();
    console.log('hd_logpresences registers: ', inst_hd_logpresences.length);

    const hd_logpresences_beacon = await hd_logpresences.find({type: "BEACON"}).toArray();
    console.log('hd_logpresences_beacon registers: ', hd_logpresences_beacon.length);

    const hd_logpresences_mobile = await hd_logpresences.find({type: "MOBILE"}).toArray();
    console.log('hd_logpresences_mobile registers: ', hd_logpresences_mobile.length);
    */

    /*
    //Se corroboran los diferentes panelistId que vienen en la colección de
    //ejemplo viewershippresences
    const panelists_viewershippresences = await viewershippresences.distinct("metadata.presence.presences.panelistId");
    console.log(panelists_viewershippresences);
    */

    /*
    //Se analiza la estructura de la colección viewershippresences de ejemplo
    //Se inicia obteniendo todos los documentos de la colección, que resulta ser uno
    const inst_viewershippresences = await viewershippresences.find();

    //Se recorre cada documento de la colección viewershippresences
    await inst_viewershippresences.forEach((document) => {
      //Se imprime el número de elementos que vienen en el arreglo metadata.presence
    	console.log('6439703686c60c0012ebd0e9 - Presencias: ' + document.metadata.presence.length);
      //Se recorre cada elmento del arreglo metadata.presence
    	document.metadata.presence.forEach((presence) => {
        //Se imprime un mensaje para saber en qué elemento del arreglo metadata.presence se está
        console.log('presence[controlNumber]:' + presence.controlNumber);
        //Se valida si existe el arreglo metadata.presence.presences
    		if (presence.presences)
    		{ //Si existe el arreglo metadata.presence.presences
          const presences_array = presence.presences; //Se guarda el arreglo netada.presence.presences en la variable presences_array
          console.log("presences:" + presences_array.length); //Se imprime un mensaje indicando el número de elementos que tiene el arreglo
          //Se busca el primero objeto que contenga el dato {panelistId: {_id: "6439703686c60c0012ebd0e9"}}
          const presences_6439703686c60c0012ebd0e9 = presences_array.find((presences) => presences.panelistId._id =="6439703686c60c0012ebd0e9");
          if (presences_6439703686c60c0012ebd0e9)
          { //Si se encuentra al menos un objeto con el dato {panelistId: {_id: "6439703686c60c0012ebd0e9"}}, se imprime
            console.log('Presencia de panelista 6439703686c60c0012ebd0e9: ', presences_6439703686c60c0012ebd0e9);
          }
          else
          {
            //Si no se encuentra objetos con el dato {panelistId: {_id: "6439703686c60c0012ebd0e9"}}, se imprimer un mensaje indicando que no se encontraron
            console.log('No hay presencias del panelista 6439703686c60c0012ebd0e9');
          }
          //Se busca el primero objeto que contenga el dato {panelistId: {_id: "6439702786c60c0012ebd09f"}}
          const presences_6439702786c60c0012ebd09f = presences_array.find((presences) => presences.panelistId._id =="6439702786c60c0012ebd09f");
          if (presences_6439702786c60c0012ebd09f)
          {
            //Si se encuentra al menos un objeto con el dato {panelistId: {_id: "6439702786c60c0012ebd09f"}}, se imprime
            console.log('Presencia de panelista 6439702786c60c0012ebd09f: ', presences_6439702786c60c0012ebd09f);
          }
          else {
            //Si no se encuentra objetos con el dato {panelistId: {_id: "6439702786c60c0012ebd09f"}}, se imprimer un mensaje indicando que no se encontraron
            console.log('No hay presencias del panelista 6439702786c60c0012ebd09f')
          }
		    }
        console.log('') //Se imprime un mensaje vacío para crear un salto de línea en consola
    	});

    });
    */

/*
    const panelists_hd_logpresences = await hd_logpresences.distinct("panelistId");
    console.log(panelists_hd_logpresences);
    await panelists_hd_logpresences.forEach(async (panelist) => {
      panelist.presences_raw = [];
      panelist.presences = [];
    });
    console.log(panelists_hd_logpresences);
    //Se consulta la colección hd_logpresences ordenada por fecha en el campo
    //timestamp, para asegurar que los datos vengan ordenados
    const hd_logpresences_by_date = hd_logpresences.find({}).sort({timestamp: 1}) //Ordena por el campo "timestamp" en orden ascendente

    //Se inicializan las variables auxiliares para segmentar las presencias por tiempos
    let previousRecord = null;
    var batches = [];
    let currentBatch = [];
    const timeThreshold = 14000; //Tiempo que dura una presencia de hd_logpresences en ms
    var timeDifference;
    var timeAct, timPrev;

    //Se barre la coleccion hd_logpresences ya ordenada por timestamp y por panelistId
    var cont = 0, contLote = 0;
    await hd_logpresences_by_date.forEach((document,i) => {
      if(previousRecord) {
        timeAct = Date.parse(document.timestamp._date);
        timePrev = Date.parse(previousRecord.timestamp._date);
        timeDifference = timeAct - timePrev;
        console.log('timeDifference: ' + timeDifference);
        if (timeDifference > timeThreshold ||  document.panelistId._id !== previousRecord.panelistId._id)
        {
          //console.log('Lote ' + contLote + ':', currentBatch);
          const panelistId = panelists_hd_logpresences.find((panelistId) => panelistId._id == document.panelistId._id);
          if (panelistId)
          {
            panelistId.presences_raw.push(currentBatch);
          }
          else {
            const none = panelists_hd_logpresences.find((panelistId) => panelistId._id == 'none');
            if (none) {
              none.presences_raw.push(currentBatch);
            }
          }
          //batches.push(currentBatch);
          currentBatch = [];
          contLote++;
        }
      }

      currentBatch.push(document);
      previousRecord = document;
    });
    //Se  muestra el último lote, si tiene datos
    if(currentBatch.length > 0) {
      const panelistId = panelists_hd_logpresences.find((panelistId) => panelistId._id == previousRecord.panelistId._id);
      if (panelistId)
      {
        panelistId.presences_raw.push(currentBatch);
      }
      else {
        const none = panelists_hd_logpresences.find((panelistId) => panelistId._id == 'none');
        if (none) {
          none.presences_raw.push(currentBatch);
        }
      }
      contLote++;
      //console.log('Lote final:', currentBatch);
    }
    */

    // console.log('batches: ' +  contLote);
    // await panelists_hd_logpresences.forEach((panelistId) => {
    //   console.log(panelistId._id + ': ', panelistId.presences_raw.length);
    // });


    // await batches.forEach((presence, i) => {
    //
    // });

    // await session.commitTransaction();
    // console.log('Transacción completada');
    // session.endSession();
  }
  // catch (e) {
  //   console.error("Error " + e + " en la función general");
  // }
  finally {
    // Ensures that the client will close when you finish/error
    await client.close();
    console.log('Conexión cerrada');
  }
}
run().catch(console.dir);

function replace_signos (str)
{
    str = str.replace(/\$oid/g, '_id');
    str = str.replace(/\$/g, '_');

    return str;
}

async function inserta_collection_json (db, collect, json_filename)
{
    const collection = db.collection(collect);
    // Leer el archivo JSON
    var data = fs.readFileSync(json_filename, 'utf8');

    data = replace_signos(data);

    const documents = JSON.parse(data);

    // Insertar los documentos en la colección
    const result = await collection.insertMany(documents);
    console.log(`${result.insertedCount} documentos insertados`);
}

function find_content_by_id (contents, id) {
    var result;
    for (let i = 0; i < contents.length; i++){
        let act_content = contents[i];
        result = act_content.find((content) => {
            return content._id._id === id;
        });
//        console.log(result);
    }
    return result;
}