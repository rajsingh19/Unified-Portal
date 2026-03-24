/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{js,jsx,ts,tsx}", "./public/index.html"],
  theme: {
    extend: {
      colors: {
        raj: {
          navy: "#222c4c",
          navySoft: "#2d3657",
          gold: "#eed5aa",
          cream: "#f7f2e7",
          paper: "#eef2f7",
          ink: "#25345f",
        },
      },
      boxShadow: {
        raj: "0 20px 45px rgba(30, 40, 74, 0.16)",
        card: "0 10px 24px rgba(47, 61, 105, 0.12)",
      },
      borderRadius: {
        xl2: "1.35rem",
      },
      fontFamily: {
        display: ["Georgia", "\"Times New Roman\"", "serif"],
        sans: ["Segoe UI", "Tahoma", "Geneva", "Verdana", "sans-serif"],
      },
    },
  },
  plugins: [],
};
