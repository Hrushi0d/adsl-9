import express from 'express';
import { Client } from 'cassandra-driver';
import mongoose from 'mongoose';
import bodyParser from 'body-parser';
import cors from 'cors';

// Initialize Express
const app = express();
const port = 3000;

// Body Parser middleware
app.use(bodyParser.json());
app.use(cors());

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

// READ all students
app.get('/cassandra', (req, res) => {
  const query = 'SELECT * FROM students_table';
  cassandraClient.execute(query)
    .then(result => res.json(result.rows))
    .catch(err => res.status(500).send('Error fetching data: ' + err));
});

// READ one student by PRN
app.get('/cassandra/:prn', (req, res) => {
  const { prn } = req.params;
  const query = 'SELECT * FROM students_table WHERE prn = ?';
  cassandraClient.execute(query, [prn], { prepare: true })
    .then(result => {
      if (result.rowLength === 0) return res.status(404).send('Student not found');
      res.json(result.rows[0]);
    })
    .catch(err => res.status(500).send('Error fetching student: ' + err));
});

// UPDATE a student by PRN
app.put('/cassandra/:prn', (req, res) => {
  const { prn } = req.params;
  const { name, department } = req.body;

  const query = 'UPDATE students_table SET name = ?, department = ? WHERE prn = ?';
  cassandraClient.execute(query, [name, department, prn], { prepare: true })
    .then(() => res.send('Student updated successfully'))
    .catch(err => res.status(500).send('Error updating student: ' + err));
});

// DELETE a student by PRN
app.delete('/cassandra/:prn', (req, res) => {
  const { prn } = req.params;

  const query = 'DELETE FROM students_table WHERE prn = ?';
  cassandraClient.execute(query, [prn], { prepare: true })
    .then(() => res.send('Student deleted successfully'))
    .catch(err => res.status(500).send('Error deleting student: ' + err));
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

// READ all students
app.get('/mongo', (req, res) => {
  StudentMongo.find()
    .then(students => res.json(students))
    .catch(err => res.status(500).send('Error fetching students: ' + err));
});

// READ one student by PRN
app.get('/mongo/:prn', (req, res) => {
  const { prn } = req.params;
  StudentMongo.findOne({ prn })
    .then(student => {
      if (!student) return res.status(404).send('Student not found');
      res.json(student);
    })
    .catch(err => res.status(500).send('Error fetching student: ' + err));
});

// UPDATE a student by PRN
app.put('/mongo/:prn', (req, res) => {
  const { prn } = req.params;
  const { name, department } = req.body;

  StudentMongo.findOneAndUpdate({ prn }, { name, department }, { new: true })
    .then(updated => {
      if (!updated) return res.status(404).send('Student not found');
      res.send('Student updated successfully');
    })
    .catch(err => res.status(500).send('Error updating student: ' + err));
});

// DELETE a student by PRN
app.delete('/mongo/:prn', (req, res) => {
  const { prn } = req.params;

  StudentMongo.findOneAndDelete({ prn })
    .then(deleted => {
      if (!deleted) return res.status(404).send('Student not found');
      res.send('Student deleted successfully');
    })
    .catch(err => res.status(500).send('Error deleting student: ' + err));
});

// Start the server
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
