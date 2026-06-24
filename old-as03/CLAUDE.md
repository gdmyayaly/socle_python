# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Angular 14 application ("as03") using Angular Material and Angular CDK for UI components. TypeScript strict mode is enabled with strict Angular template checking.

## Commands

- **Dev server:** `npm start` (serves at http://localhost:4200, hot-reload enabled)
- **Build:** `npm run build` (production output in `dist/as03/`)
- **Build (watch):** `npm run watch`
- **Unit tests:** `npm test` (Karma + Jasmine, launches Chrome)
- **Generate component:** `npx ng generate component <name>`
- **Generate other:** `npx ng generate directive|pipe|service|class|guard|interface|enum|module <name>`

## Architecture

- **NgModule-based** app (not standalone components) — `AppModule` is the root module bootstrapped in `main.ts`
- **Routing:** `AppRoutingModule` handles routes (currently empty)
- **UI library:** Angular Material with the `indigo-pink` prebuilt theme, loaded via `styles` in `angular.json`
- **Component prefix:** `app` (enforced in `angular.json`)

## TypeScript Configuration

Strict mode with additional flags: `noImplicitOverride`, `noPropertyAccessFromIndexSignature`, `noImplicitReturns`, `noFallthroughCasesInSwitch`. Angular compiler options include `strictTemplates` and `strictInjectionParameters`.

## Known Issues

There are `@types/node` compatibility errors with `Symbol.dispose` (see `error.txt`). These come from a newer `@types/node` version conflicting with TypeScript 4.7. If encountered, pin `@types/node` to a version compatible with TS 4.7.
