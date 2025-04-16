import express from 'express';
import { Client } from 'cassandra-driver';
import mongoose from 'mongoose';
import bodyParser from 'body-parser';

// Initialize Express
const app = express();
const port = 3000;

// Body Parser middleware
app.use(bodyParser.json());

// Cassandra setup
const cassandraClient = new Client({
  contactPoints: ['localhost'], // Replace with your Cassandra node address
  localDataCenter: 'datacenter1',
  keyspace: 'prn22510109', // Make sure this keyspace exists
});

cassandraClient.connect(err => {
  if (err) {
    console.error('Error connecting to Cassandra:', err);
  } else {
    console.log('Connected to Cassandra');
  }
});

// MongoDB setup
mongoose.connect('mongodb://localhost:27017/students', { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB'))
  .catch(err => console.log('Error connecting to MongoDB:', err));

// MongoDB Schema
const studentSchema = new mongoose.Schema({
  name: String,
  prn: String,
  department: String,
});

const StudentMongo = mongoose.model('Student', studentSchema);

// POST to Cassandra
app.post('/cassandra', (req, res) => {
  const { name, prn, department } = req.body;

  const query = 'INSERT INTO students_table (name, prn, department) VALUES (?, ?, ?)';
  cassandraClient.execute(query, [name, prn, department], { prepare: true })
    .then(() => {
      res.status(200).send('Data inserted into Cassandra');
    })
    .catch(err => {
      res.status(500).send('Error inserting into Cassandra: ' + err);
    });
});

// POST to MongoDB
app.post('/mongo', (req, res) => {
  const { name, prn, department } = req.body;

  const newStudent = new StudentMongo({
    name,
    prn,
    department
  });

  newStudent.save()
    .then(() => {
      res.status(200).send('Data inserted into MongoDB');
    })
    .catch(err => {
      res.status(500).send('Error inserting into MongoDB: ' + err);
    });
});

// Start the server
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
