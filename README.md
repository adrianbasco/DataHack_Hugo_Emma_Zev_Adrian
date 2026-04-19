# DataHack_Hugo_Emma_Zev_Adrian


**LLM GENERATED**

# Frontend Setup and Run Guide

This project uses **Expo + React Native** for the frontend.

## Prerequisites

Before running the frontend, make sure you have:

- **Node.js** installed
- **npm** installed
- the project cloned locally
- a terminal open in the `date-night-app` folder

Optional, depending on how you want to run it:

- **Expo Go** on your phone, if you want to test on a physical device
- **Android Studio** with an emulator, if you want to test on an Android emulator

---
<!-- .\venv\Scripts\Activate.ps1 -->

## 1. Open the frontend folder

If you are in the main project folder, go into the Expo app:

cd date-night-app

##  2. Install dependencies

Run:

npm install

npx expo install react-dom react-native-web

## 3. running the app in browser (fastest)
npm run web

## 3.1 runnning the app on phone
Install Expo Go on your phone
Start the project:
npx expo start
Scan the QR code using Expo Go

## 3.2 running the app on Android emulator
Run on an Android emulator

This requires Android Studio and an emulator to be installed and configured.

Start the Expo server:

npx expo start

Then press:

a

in the terminal to open the app on the Android emulator.