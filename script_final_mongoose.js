const { MongoClient, ServerApiVersion } = require('mongodb');
const fs = require('fs');
const mongoose = require('mongoose');

// URL de conexión a la base de datos
const uri = 'mongodb+srv://caraven90:EqlgdNhlriHGrugS@cluster0.mg6hkhi.mongodb.net/?retryWrites=true&w=majority';

// Configuración de Mongoose
mongoose.connect(uri, { useNewUrlParser: true, useUnifiedTopology: true });

const sch_granular = new mongoose.Schema({
    _id : {
        _id : {
            type: mongoose.Schema.Types.ObjectId,
            default: new mongoose.Types.ObjectId() // Genera un nuevo ObjectId por defecto
        }
    },
    start :{
        _date : String
    },
    end :{
        _date : String
    },
    controlNumber : Number,
    timeInSecods : Number,
    timeInMinutes: Number,
    panelistId : {
        _id : {
            type: mongoose.Schema.Types.ObjectId
        }
    },
    type : String,
    personalDeviceId : {
        _id : {
            type: mongoose.Schema.Types.ObjectId
        }
    },
    contentId : {
        _id : {
            type: mongoose.Schema.Types.ObjectId
        }
    }
});

const sch_presences = new mongoose.Schema({
    start :{
        _date : String
    },
    end :{
        _date : String
    },
    granular : [sch_granular],
    panelistId : {
        _id : {
            type: mongoose.Schema.Types.ObjectId
        }
    },
    controlNumber : Number,
    timeInSecods : Number,
    timeInMinutes: Number,
    contentId : {
        _id : {
            type: mongoose.Schema.Types.ObjectId
        }
    },
    contentType : String,
    viewingSource: String,
    contentCatalogId: {
        _id : {
            type: mongoose.Schema.Types.ObjectId
        }
    },
    mediaSourceId : {
        _id : {
            type: mongoose.Schema.Types.ObjectId
        }
    },
    providerId : {
        _id : {
            type: mongoose.Schema.Types.ObjectId
        }
    },
    _id : {
        _id : {
            type: mongoose.Schema.Types.ObjectId,
            default: new mongoose.Types.ObjectId() // Genera un nuevo ObjectId por defecto
        }
    }
});

const sch_presence = new mongoose.Schema({
    // Definición del esquema para el subdocumento
    _id: {
        _id : {
            type: mongoose.Schema.Types.ObjectId,
            default: new mongoose.Types.ObjectId() // Genera un nuevo ObjectId por defecto
        }
    },
    controlNumber : Number,
    presences: [sch_presences]
});

const sch_viewershippresences = new mongoose.Schema({
    _id: {
        _id : {
            type: mongoose.Schema.Types.ObjectId,
            default: new mongoose.Types.ObjectId() // Genera un nuevo ObjectId por defecto
        }
    },
    timestamp : {
        _date : String
    },
    metadata : {
        householdId : {
            _id : {
                type: mongoose.Schema.Types.ObjectId,
            }
        },
        coreMeterId : {
            _id : {
                type: mongoose.Schema.Types.ObjectId,
            }
        },
        presence : [sch_presence],
        createdAt : {
            _date : String
        },
        updatedAt : {
            _date : String
        }
    }
});

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: false,
    deprecationErrors: true,
  }
});

const dbName = 'ejercicio';

const collection = mongoose.model('viewershippresences', sch_viewershippresences);

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
    
    //Variables para manipular las colecciones
    const hd_logpresences = db.collection("hd_logpresences");
    const viewershipcontents = db.collection("viewershipcontents");
    const viewershippresences = db.collection('viewershippresences_example');

    //Se instancian la colección viewershipcontents
    const inst_viewershipcontents = await viewershipcontents.find({});
    var contents = [];
    var timestamp, householdId, coreMeterId;
    
    await inst_viewershipcontents.forEach((viewershipcontent) => {
       contents.push(viewershipcontent.metadata.contents);
//       console.log(viewershipcontent.metadata.householdId);
       timestamp =  viewershipcontent.metadata.timestamp;
       householdId = viewershipcontent.metadata.householdId;
       coreMeterId = viewershipcontent.metadata.coreMeterId;
    });
//    console.log(contents);
    
    var viewershippresences_doc = new Object();
    viewershippresences_doc.timestamp = timestamp;
    viewershippresences_doc.metadata = new Object();
//    console.log(householdId);
    viewershippresences_doc.metadata.householdId = householdId;
    viewershippresences_doc.metadata.coreMeterId = coreMeterId;
    viewershippresences_doc.metadata.presence = [];
    viewershippresences_doc.metadata.createdAt = new Object();
    viewershippresences_doc.metadata.createdAt._date = new Date().toISOString();
    viewershippresences_doc.metadata.updatedAt = new Object();
    viewershippresences_doc.metadata.updatedAt._date = new Date().toISOString();
    viewershippresences_doc.metadata.presence = await createPresenceArray(hd_logpresences, contents);
    
    
    
//    console.log(new_viewershippresences);
    try
    {
        const resultado = await collection.create(viewershippresences_doc);
        console.log('Documento creado con Mongoose:', resultado);
    }
    catch(e) {
        console.error('Error mongoose:' , e);
    }
  }
  finally {
    // Ensures that the client will close when you finish/error
    await client.close();
    console.log('Conexión cerrada');//        console.log(panelist.presences_raw);
//        console.log(panelist.presences_raw.length);
// Cerrar la conexión de Mongoose
    mongoose.connection.close();
  }
}
run().catch(console.dir);

function replace_signos (str)
{
    str = str.replace(/\$oid/g, '_id');
    str = str.replace(/\$/g, '_');

    return str;
}

async function inserta_collection_json_file (db, collect, json_filename)
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

function filter_content_by_date (contents, date_from, date_to) {
    var result;
    for (let i = 0; i < contents.length; i++){
        let act_content = contents[i];
        result = act_content.filter((content) => {
            let start = new Date(content.start._date);
            let end = new Date(content.end._date);
            let from = new Date (date_from);
            let to = new Date (date_to)
            return start.getTime() >= from.getTime() && end.getTime() <= to.getTime()
        });
    }
    return result;
}

function find_content_by_timestamp (contents, timestamp, timeThreshold) {
    var result;
    for (let i = 0; i < contents.length; i++){
        let act_content = contents[i];
        result = act_content.find((content) => {
            let start = new Date(content.start._date);
            let end = new Date(content.end._date);
            let from = new Date (timestamp);
            let to = new Date(from.setMilliseconds(from.getMilliseconds() +  timeThreshold));
//            console.log(' start:' + start.toISOString() + ', end:' + end.toISOString());
//            console.log(' from:' + from.toISOString() + ', to: ' + to.toISOString());
            return from.getTime() >= start.getTime() && to.getTime() <= end.getTime();
        });
//        console.log(result);
    }
    return result;
}

async function createPresenceArray (hd_logpresences, contents) {
    //Se analizan los diferentes panelistId que vienen en la colección hd_logpresences
    const panelists_hd_logpresences = await hd_logpresences.distinct("panelistId");
    var presence = [];
    //Se recorren los espacios de los 12 panelistas predefinidos
    for (let i = 0; i < 12; i++) {   
        presence[i] = new Object();
//        presence[i]._id = new Object();
        presence[i].controlNumber = i + 1;
        if (panelists_hd_logpresences[i]) {
            presence[i].presences = [];
            let panelist = panelists_hd_logpresences[i];            
            let previousRecord = null;
            let currentBatch = [];
            let presencesBatch = [];
            let presences_granular = new Object();
            let aux_end_gran_pres;
            let timeDifference;
            let timeAct, timePrev;
            const timeThreshold = 15000; //Tiempo que dura una presencia de hd_logpresences en ms
            const tolerancia = 5000; //Se agrega una tolerancia
            let start, end;
            let content_presence_act, content_presence_prev;
            let type_act, type_prev;
            let personalDeviceId_act, personalDeviceId_prev;
            let aux_end;
            let timeInMiliseconds;
            //Se obtienen los registros de presencia del panelista ordenados por fecha
            const panelist_presences_by_date = hd_logpresences.find({'panelistId._id': panelist._id}).sort({timestamp: 1}); //Ordena por el campo "timestamp" en orden ascendente

            //Se recorren los registros del panelista actual
            await panelist_presences_by_date.forEach((document) => {
                content_presence_act = find_content_by_timestamp(contents, document.timestamp._date, timeThreshold);
                type_act = document.type;
                personalDeviceId_act = document.personalDeviceId._id;
                if(previousRecord) {
                    timeAct = Date.parse(document.timestamp._date);
                    timePrev = Date.parse(previousRecord.timestamp._date);
                    timeDifference = timeAct - timePrev;
    //                console.log(document.timestamp._date);
                    if ((timeDifference > (timeThreshold + tolerancia)) || 
                        (content_presence_act !== content_presence_prev))
                    {
    //                    console.log("Diferente presencia: " + timeDifference);
                        aux_end = new Date(previousRecord.timestamp._date);
                        end = new Date(aux_end.setMilliseconds(aux_end.getMilliseconds() +  timeThreshold)).toISOString();
                        presencesBatch.end._date = end;
                        presencesBatch.panelistId = document.panelistId;
                        presencesBatch.controlNumber = i + 1;
                        timeInMiliseconds = Date.parse(presencesBatch.end._date) - Date.parse(presencesBatch.start._date);
                        presencesBatch.timeInSeconds = timeInMiliseconds/1000;
                        presencesBatch.timeInMinutes = presencesBatch.timeInSeconds/60;
                        if (content_presence_prev) 
                        {
                            presencesBatch.contentId = content_presence_prev._id;
                            presencesBatch.contentType = content_presence_prev.contentType;
                            presencesBatch.viewingSource = content_presence_prev.viewingSource;
                            if(content_presence_prev.contentCatalog) {
                                presencesBatch.contentCatalogId = content_presence_prev.contentCatalog.id;
                            }
                            presencesBatch.mediaSourceId = content_presence_prev.mediaSource.id;
                            presencesBatch.providerId = content_presence_prev.provider.id;
                        }
                        //Se actualizan los datos granurales
                        presences_granular.end = new Object();
                        presences_granular.end._date = end;
                        presences_granular.controlNumber = i + 1;
                        presences_granularTimeInMiliseconds = Date.parse(presences_granular.end._date) - Date.parse(presences_granular.start._date);
                        presences_granular.timeInSeconds = presences_granularTimeInMiliseconds/1000;
                        presences_granular.timeInMinutes = presences_granular.timeInSeconds/60;
                        presences_granular.panelistId = document.panelistId;
                        if (content_presence_prev)
                        {
                            presences_granular.type = content_presence_prev.contentType;
                            presences_granular.personaDeviceId = document.personalDeviceId._id;
                            presences_granular.contentId = content_presence_prev._id;
                        }
//                        console.log(presencesBatch.granular);
                        presencesBatch.granular.push(presences_granular);
                        //Se guarda el currentBatch
//                        panelist.presences.push(presencesBatch);
                        presence[i].presences.push(presencesBatch);
                        presencesBatch = [];
                        start = document.timestamp._date;
                        presencesBatch.start = new Object();
                        presencesBatch.start._date = start;
                        presencesBatch.end = new Object();
                        //Se arma el primer objeto del arreglo granular
                        presencesBatch.granular = [];
                        presences_granular = new Object();
                        presences_granular.start = presencesBatch.start;

                    }
                    else if (type_act !== type_prev || personalDeviceId_act !== personalDeviceId_prev)
                    {
                        //Se actualizan los datos granurales
                        aux_end_gran_pres = new Date(previousRecord.timestamp._date);
                        presences_granular.end = new Object();
                        presences_granular.end._date = new Date(aux_end_gran_pres.setMilliseconds(aux_end_gran_pres.getMilliseconds() +  timeThreshold)).toISOString();
                        presences_granular.controlNumber = i +1;
                        presences_granularTimeInMiliseconds = Date.parse(presences_granular.end._date) - Date.parse(presences_granular.start._date);
                        presences_granular.timeInSeconds = presences_granularTimeInMiliseconds/1000;
                        presences_granular.timeInMinutes = presences_granular.timeInSeconds/60;
                        presences_granular.panelistId = document.panelistId;
                        if (content_presence_prev)
                        {
                            presences_granular.type = content_presence_prev.contentType;
                            presences_granular.personalDeviceId = document.personalDeviceId._id;
                            presences_granular.contentId = content_presence_prev._id;
                        }
                        presencesBatch.granular.push(presences_granular);
//                        console.log(presencesBatch.granular);
                        presences_granular = new Object();
                        presences_granular.start = new Object();
                        presences_granular.start._date = document.timestamp._date;
                    }
                }
                else {
                    start = document.timestamp._date;
                    presencesBatch.start = new Object();
                    presencesBatch.start._date = start;
                    presencesBatch.end  = new Object();
                    //Se arma el primer objeto del arreglo granular
                    presencesBatch.granular = [];
                    presences_granular = new Object();
                    presences_granular.start = presencesBatch.start;

                }
                currentBatch.push(document);
                previousRecord = document;
                content_presence_prev = content_presence_act;
                type_prev = type_act;
                personalDeviceId_prev = personalDeviceId_act;
            });

            if(currentBatch.length > 0) {
                aux_end_gran_pres = new Date(previousRecord.timestamp._date);
                presencesBatch.end._date = new Date(aux_end_gran_pres.setMilliseconds(aux_end_gran_pres.getMilliseconds() +  timeThreshold)).toISOString();
                presencesBatch.granular = [];
                presencesBatch.panelistId = previousRecord.panelistId;
                presencesBatch.controlNumber = i + 1;
                timeInMiliseconds = Date.parse(presencesBatch.end._date) - Date.parse(presencesBatch.start._date);
                presencesBatch.timeInSeconds = timeInMiliseconds/1000;
                presencesBatch.timeInMinutes = presencesBatch.timeInSeconds/60;
                if (content_presence_act) 
                {
                    presencesBatch.contentId = content_presence_act._id;
                    presencesBatch.contentType = content_presence_act.contentType;
                    presencesBatch.viewingSource = content_presence_act.viewingSource;
                    if(content_presence_act.contentCatalog) {
                        presencesBatch.contentCatalogId = content_presence_act.contentCatalog.id;
                    }
                    presencesBatch.mediaSourceId = content_presence_act.mediaSource.id;
                    presencesBatch.providerId = content_presence_act.provider.id;
                }
                //Se actualizan los datos granurales
                presences_granular.end = presencesBatch.end;
                presences_granular.controlNumber = i +1;
                presences_granularTimeInMiliseconds = Date.parse(presences_granular.end._date) - Date.parse(presences_granular.start._date);
                presences_granular.timeInSeconds = presences_granularTimeInMiliseconds/1000;
                presences_granular.timeInMinutes = presences_granular.timeInSeconds/60;
                presences_granular.panelistId = previousRecord.panelistId;
                if (content_presence_prev)
                {
                    presences_granular.type = content_presence_prev.contentType;
                    presences_granular.personalDeviceId = previousRecord.personalDeviceId._id;
                    presences_granular.contentId = content_presence_prev._id;
                }
                presencesBatch.granular.push(presences_granular);
                presence[i].presences.push(presencesBatch);

            }
//            console.log('presences [' +i + '] len: ', presences[i].length);
//            console.log('presences[' + i + ']: ', presence[i].presences);
        }
    }
    
    return presence;
}