# dynatask-rs

A system for managing dependent and asynchronous job processing. Available in Rust and Python, it ensures great flexibility, covering scenarios where you need performance and low-level control (Rust) and those requiring rapid development, extensive library support, and scripting capabilities (Python). Using task types, different applications can each handle a specific task subsets. A Rust application may handle some tasks, while a Python one handles other tasks.

## 🏗️ Core Components and Design
A few key concepts:

- Initial Task Creation: A job starts when its initial task is created. This is the entry point for any job.
- Task Chaining/Nesting: The ability for any running task to dynamically create new, dependent tasks (additional tasks). A task can wait for its sub-tasks, sub-sub-tasks, and so on, to complete. 
- Job Completion Tracking: A mechanism to ensure the parent Job state remains "In Progress" until the very last descendant task completes.

## 💡 Potential Use Cases
This pattern is quite versatile. Here are a few domains where this architecture is particularly effective:

| Domain | Example Job | Task Chaining Example|
| --- | --- | --- |
| Data Processing | Ingest and process a large dataset file. | <ol><li>Initial Task: Read file metadata.</li><li>Additional Tasks: Split the file into 100 chunks and create a 'Process Chunk' task for each.</li><li>Nested Tasks: A 'Process Chunk' task might create a 'Validate Record' task for any corrupted data it finds.</li></ol> |
| Web Scraping | Scrape a website starting from an index page. | <ol><li>Initial Task: Fetch the index page HTML.</li><li>Additional Tasks: Parse the HTML, extract links, and create a 'Fetch Link' task for each unique URL found.</li><li>Nested Tasks: A 'Fetch Link' task might find a "Next Page" button and create a new 'Fetch Link' task for the next page.</li></ol> |
| CI/CD Pipelines	| Execute a full software build and deploy.	| <ol><li>Initial Task: Start the build.</li><li>Additional Tasks: Create parallel tasks for 'Run Unit Tests', 'Run Integration Tests', and 'Build Artifacts'.</li><li>Nested Tasks: The 'Build Artifacts' task, once complete, creates a 'Deploy to Staging' task, which only runs if tests passed.</li></ol> |
| Distributed Computing |	Render a complex 3D scene or video.	| <ol><li>Initial Task: Define render settings and scene breakdown.</li><li>Additional Tasks: Create a 'Render Frame' task for every 10 frames of the video.</li><li>Nested Tasks: A 'Render Frame' task might create 'Post-Process Image' tasks to apply filters to the completed image.</li></ol> |
