(function(){
  var KEY='casa_checkout_progress';
  var MAX_AGE=1000*60*60*24*14;
  function fallback(){
    return '/#produtos';
  }
  function smartCartUrl(){
    try{
      var raw=localStorage.getItem(KEY);
      if(!raw)return fallback();
      var data=JSON.parse(raw);
      var updated=data&&data.updatedAt?new Date(data.updatedAt).getTime():0;
      if(!data||!data.href||!updated||Date.now()-updated>MAX_AGE)return fallback();
      return data.href;
    }catch(e){
      return fallback();
    }
  }
  function init(){
    document.querySelectorAll('.cart').forEach(function(cart){
      var url=smartCartUrl();
      if(cart.tagName==='A')cart.setAttribute('href',url);
      cart.addEventListener('click',function(event){
        event.preventDefault();
        location.href=smartCartUrl();
      });
    });
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);
  else init();
})();
