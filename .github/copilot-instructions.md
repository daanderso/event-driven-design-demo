# Java Backend Project Rules

## General Principles
- Follow Spring Boot best practices
- Keep code simple and maintainable
- Prefer standard conventions over custom abstractions

## Repository Hygiene (.gitignore)
- Always maintain a proper .gitignore file for Java/Spring Boot projects
- Never commit:
    - /target/
    - IDE files (.idea, .vscode, *.iml)
    - logs (*.log)
    - environment files (.env, application-local.yml secrets)
    - OS files (.DS_Store, Thumbs.db)
- Ensure build artifacts and secrets are excluded from version control
- If missing, suggest an appropriate .gitignore for the project

## Architecture
Use layered architecture:
controller → service → repository → entity → dto → mapper → config → exception

## Dependency Injection
- Always use constructor injection
- Never use field injection

## API Design
- Keep controllers thin
- Business logic belongs in services
- Use DTOs for all API contracts

## Validation
- Use Jakarta Validation annotations on DTOs

## Error Handling
- Use @RestControllerAdvice for global exception handling

## Logging
- Use SLF4J only
- Never use System.out.println

## Database
- PostgreSQL for production
- H2 for local development

## Testing
- JUnit 5 + Mockito for unit tests
- Testcontainers for integration tests

## Mapping
- Prefer MapStruct over manual mapping

## Security
- Never hardcode secrets
- Follow Spring Security best practices

## Simplicity Rule
Prefer the simplest solution that meets requirements.
Avoid overengineering.