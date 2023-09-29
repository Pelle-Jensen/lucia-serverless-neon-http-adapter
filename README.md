# lucia-serverless-neon-http-adapter
Adapter for Lucia to connect with pure http neon serverless - Mainly for use with environments where websockets are unavailable (SvelteKit for example)

Based on the [neon adapter by MightyPart](https://github.com/MightyPart/lucia-serverless-neon-adapter)

## Installation
```
npm i lucia-serverless-neon-http-adapter
```

## Usage
```ts
adapter: neon(NEON_DATABASE_URL, {
    user: 'user',
    key: 'user_key',
    session: 'user_session'
}),
```
