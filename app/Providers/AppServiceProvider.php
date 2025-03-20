<?php

namespace App\Providers;

use Illuminate\Process\Factory;
use Illuminate\Support\ServiceProvider;

class AppServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     */
    public function register(): void
    {
        //
    }

    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        app()->bind(Factory::class, function () {
            return new class extends Factory
            {
                public function newPendingProcess()
                {
                    return parent::newPendingProcess()->forever();
                }
            };
        });
    }
}
