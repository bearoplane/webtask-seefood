"use latest";

import { parallel } from 'async';
import { MongoClient } from 'mongodb';
import request from 'request';

const API_URL = 'http://api.yummly.com/v1/api';

function get_cached_recipe(recipe, db, cb) {
  const q = {
    recipe: recipe
  };

  db.collection('recipes')
  .findOne(q, (err, res) => {
    if (err) {
      return cb(err);
    }
    
    cb(null, res || {});
  });
}
function merge_ingredients(a, b) {
  let merged = Object.assign({}, a);
  Object.keys(b).forEach(key => {
    merged[key] = merged[key] || 0;
    merged[key] += b[key];
  });
  return merged;
}

export default function(ctx, wt_cb) {
  const q = ctx.data.q;
  
  MongoClient.connect(ctx.data.MONGO_URL, function (err, db) {
    if (err) {
      return done(error);
    }
      
    get_cached_recipe(q, db, (err, recipe) => {
      const qs = {
        q: q,
        start: recipe.count || 0
      };
      const headers = {
        'X-Yummly-App-ID': ctx.data.YUMMLY_APP_ID,
        'X-Yummly-App-Key': ctx.data.YUMMLY_APP_KEY
      };
      
      request({
        method: 'GET',
        url: `${API_URL}/recipes`,
        headers,
        qs
      }, function (error, res, body) {
        const parsedBody = JSON.parse(body);
        const recipes = parsedBody.matches;
        
        const resultCount = recipes.length;
        const results = recipes.reduce((ret, val) => {
          val.ingredients.forEach((ingredient) => {
            ret[ingredient] = ret[ingredient] || 0;
            ret[ingredient]++;
          });
          return ret;
        }, {});
        
        const mergedIngredients = merge_ingredients(recipe.ingredients, results);
        
        const filter = {
          recipe: q
        };
        const update = {
          $set: {
            recipe: q,
            ingredients: mergedIngredients
          },
          $inc: {
            count: resultCount
          }
        };
        const opts = { upsert: true };
        
        db.collection('recipes')
        .updateOne(filter, update, opts, (err, res) => {
          if (err) {
            wt_cb(err);
          }
          
          const sortedIngredients = Object.keys(mergedIngredients).sort((a, b) => {
            return (mergedIngredients[b] - mergedIngredients[a]);
          }).map((key) => ({ingredient: key, count: mergedIngredients[key]}));
          
          wt_cb(null, {
            recipeCount: resultCount + (recipe.count || 0),
            ingredients: sortedIngredients
          });
        });
      });
    });
  });
}