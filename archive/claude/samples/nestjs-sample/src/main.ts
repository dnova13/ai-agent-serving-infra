import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // React 등 다른 오리진에서 오는 요청을 허용
  app.enableCors();

  // DTO에 정의된 규칙으로 요청 데이터를 자동 검증
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));

  await app.listen(3000);
  console.log('NestJS server is running on http://localhost:3000');
}
bootstrap();
