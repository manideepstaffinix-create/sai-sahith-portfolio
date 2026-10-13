const fs = require('fs');
const path = require('path');

const dir = __dirname;
const files = ['index.html', 'styles.css', 'app.js'];

let allGood = true;

files.forEach(file => {
  const fullPath = path.join(dir, file);
  const content = fs.readFileSync(fullPath, 'utf8');

  // Check em dash (\u2014) and en dash (\u2013)
  const emDashCount = (content.match(/[\u2014\u2013]/g) || []).length;
  
  // Check emojis (typical range)
  const emojiRegex = /[\u{1F300}-\u{1F9FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}]/u;
  const emojiMatches = (content.match(emojiRegex) || []);

  // Check purple references in CSS/styles
  const purpleMatches = (content.match(/purple|rgba?\(.*?(128|138|148).*?\)/gi) || []);

  console.log(`[AUDIT] ${file}:`);
  console.log(` - Em/En Dashes: ${emDashCount}`);
  console.log(` - Emoji Matches: ${emojiMatches.length}`);
  console.log(` - Purple mentions: ${purpleMatches.length}`);

  if (emDashCount > 0) {
    console.log(`   WARNING: Found dashes in ${file}`);
    allGood = false;
  }
  if (emojiMatches.length > 0) {
    console.log(`   WARNING: Found emojis in ${file}`);
    allGood = false;
  }
});

console.log('Overall Audit Result:', allGood ? 'PERFECT PASS' : 'ISSUES DETECTED');
