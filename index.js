//const {Storage} = require('@google-cloud/storage');
const {BigQuery} = require('@google-cloud/bigquery');

const options = {
    keyFilename: '/path/to/keyfile.json',
    projectId: 'contact-form-253313',
  };

//const storage = new Storage();
const bigquery = new BigQuery(options);

const bucketName = 'big-query-upload';
const fileName = 'Maryland_Operating_Budget.csv';
const datasetID = 'MD_Operating_Budget';
const tableID = 'MD_Operating_Budget';

schema = [
  {name: 'Organization_Code', type: 'STRING'},
  {name: 'Organization_Sub_Code', type: 'STRING'},
  {name: 'Type', type: 'STRING'},
  {name: 'Fiscal_Year', type: 'STRING'},
  {name: 'Agency_Code', type: 'STRING'},
  {name: 'Unit_Code', type: 'STRING'},
  {name: 'Unit_Name', type: 'STRING'},
  {name: 'Program_Code', type: 'STRING'},
  {name: 'Program_Name', type: 'STRING'},
  {name: 'Subprogram_Code', type: 'STRING'},
  {name: 'Subprogram_Name', type: 'STRING'},
  {name: 'Object_Code', type: 'STRING'},
  {name: 'Object_Name', type: 'STRING'},
  {name: 'Comptroller_Subobject_Code', type: 'STRING'},
  {name: 'Agency_Subobject_Code', type: 'STRING'},
  {name: 'Agency_Subobject_Name', type: 'STRING'},
  {name: 'Fund_Type_Name', type: 'STRING'},
  {name: 'Budget', type: 'INTEGER'},
  {name: 'Descripting', type: 'STRING'},
  {name: 'Category', type: 'INTEGER'},
  {name: 'Category_Title', type: 'STRING'}
]


async function createBucket(){
  await storage.createBucket(bucketName);
  console.log(`Bucket ${bucketName} created.`);
}

async function uploadFile(){
  await storage.bucket(bucketName).upload(fileName, {
    gzip: true,
    metadata: {
      cacheControl: 'public, max-age=31536000',
    },
  });

  console.log(`${fileName} uploaded to ${bucketName}.`);
}

async function createDataset(){
 const dsOptions ={
     location: 'US',
 };

 const [dataset] = await bigquery.createDataset(datasetID, dsOptions);
 console.log(`Dataset ${dataset.id} created.`); 
}

async function createTable(){
  
 const options = {
   schema: schema,
   location: 'US',
 };

 const [table] = await bigquery
  .dataset(datasetID)
  .createTable(tableID, options);

  console.log(`Table ${table.id} created.`);

}

async function loadCSVFromGCS(){
  const metadata = {
    sourceFormat: 'CSV',
    skipLeadingRows: 1,
    schema: schema
  };

  const [job] = await bigquery
    .dataset(datasetID)
    .table(tableID)
    .load(storage.bucket(bucketName).file(fileName), metadata);

  console.log(`Job ${job.id} completed.`);

  const errors = job.status.errors;
  if(errors && errors.length >  0){
    throw errors;
  }
}

//createDataset();
//createTable();
