# UC Admission Statistics Application

This Flask-based application processes and analyzes UC (University of California) admission statistics. It allows users to upload Excel files containing admission data and query the processed information.

## Project Structure

## Getting Started

To run this project using Docker, follow these steps:

1. Make sure you have Docker and Docker Compose installed on your system.

2. Clone this repository to your local machine:
   ```
   git clone <repository-url>
   cd <project-directory>
   ```

3. Create a `.env` file in the project root with the following content:
   ```
   DATABASE_URL=postgresql://user:password@db:5432/uc_admissions
   SECRET_KEY=your_secret_key_here
   ```

4. Build and start the Docker containers:
   ```
   docker-compose up --build
   ```

5. Open your web browser and navigate to `http://localhost:5000` to access the application.

To stop the application, press `Ctrl+C` in the terminal where docker-compose is running.

To shut down the containers and remove volumes, run: