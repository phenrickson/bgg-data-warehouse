# Contributing to BGG Data Warehouse

Thank you for your interest in contributing to the BGG Data Warehouse project! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct. Please read it before contributing.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/yourusername/bgg-data-warehouse.git
   cd bgg-data-warehouse
   ```
3. Set up your development environment:
   ```bash
   # Create and activate a virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   make install
   ```

## Development Process

1. Create a new branch for your feature/fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes, following our coding standards:
   - Use type hints
   - Follow PEP 8 style guide
   - Write docstrings for functions and classes
   - Add tests for new functionality

3. Run tests and linting:
   ```bash
   make test
   make lint
   ```

4. Commit your changes:
   ```bash
   git add .
   git commit -m "feat: description of your changes"
   ```
   
   Follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages:
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `test:` for test changes
   - `refactor:` for code refactoring
   - `style:` for formatting changes
   - `chore:` for maintenance tasks

5. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

6. Create a Pull Request

## Pull Request Process

1. Update the README.md with details of changes if applicable
2. Update the documentation if you're changing functionality
3. Add tests for new features
4. Ensure all tests pass and there are no linting errors
5. Update the example configuration if needed
6. Link any relevant issues in the PR description

## Testing

- Write unit tests for new functionality
- Ensure existing tests pass
- Add integration tests for new features
- Test edge cases and error conditions

## Code Style

We use several tools to maintain code quality:

- `black` for code formatting
- `ruff` for linting
- `mypy` for type checking

Configuration for these tools is in `pyproject.toml`.

## Documentation

- Update docstrings for modified functions/classes
- Keep docstrings in Google style
- Update README.md for significant changes
- Add comments for complex logic

## Working with BigQuery

When making changes to BigQuery-related code:

1. Test locally with a development project
2. Document any schema changes
3. Provide migration scripts if needed
4. Consider backward compatibility
5. Test data validation

## API Guidelines

When working with the BGG API:

1. Respect rate limits (2 requests/second)
2. Handle errors gracefully
3. Log API interactions appropriately
4. Cache responses when possible
5. Follow BGG's terms of service

## Release Process

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create a release PR
4. Tag the release after merge
5. Update documentation

## Getting Help

- Create an issue for bugs or feature requests
- Join our discussions for questions
- Check existing issues and PRs before creating new ones

## Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
# Edit .env with your settings
```

Required variables:
- `GCP_PROJECT_ID`
- `GCS_BUCKET`
- `GOOGLE_APPLICATION_CREDENTIALS`

## Project Structure

```
bgg-data-warehouse/
├── src/                 # Source code
│   ├── api_client/     # BGG API client
│   ├── data_processor/ # Data transformation
│   ├── id_fetcher/     # Game ID management
│   ├── pipeline/       # Main pipeline logic
│   ├── quality_monitor/# Data quality checks
│   ├── visualization/  # Data visualization
│   └── warehouse/      # BigQuery integration
├── tests/              # Test suite
├── config/             # Configuration files
└── docs/              # Documentation
```

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
